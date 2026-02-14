import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

type SupabaseClient = ReturnType<typeof createClient>;

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

/**
 * run-full-sync - ORCHESTRATORE LEGGERO
 * 
 * Non esegue logica di business, solo orchestrazione.
 * Chiama step separati tramite sync-step-runner per evitare WORKER_LIMIT.
 * 
 * SFTP upload: SOLO per trigger === 'cron'.
 * File SFTP: Export Mediaworld.csv, Export ePrice.csv, catalogo_ean.xlsx,
 *            amazon_listing_loader.xlsm, amazon_price_inventory.txt
 * 
 * Storage versioning: latest/ (overwrite) + versions/<timestamp>/ (storico)
 * Retention: per ciascuno dei 5 file, max 3 versioni; elimina >3 solo se >7 giorni.
 * 
 * success_with_warning: basato SOLO su warning_count > 0.
 */

const MAX_PARSE_MERGE_CHUNKS = 100;

// The 5 canonical export files
const EXPORT_FILES = [
  'Export Mediaworld.csv',
  'Export ePrice.csv',
  'catalogo_ean.xlsx',
  'amazon_listing_loader.xlsm',
  'amazon_price_inventory.txt'
];

async function callStep(supabaseUrl: string, serviceKey: string, functionName: string, body: Record<string, unknown>): Promise<{ success: boolean; error?: string; data?: Record<string, unknown> }> {
  try {
    console.log(`[orchestrator] Calling ${functionName}...`);
    const resp = await fetch(`${supabaseUrl}/functions/v1/${functionName}`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    
    if (!resp.ok) {
      const errorText = await resp.text().catch(() => 'Unknown error');
      console.log(`[orchestrator] ${functionName} HTTP error ${resp.status}: ${errorText}`);
      
      // Detect WORKER_LIMIT (HTTP 546)
      if (resp.status === 546 || errorText.includes('WORKER_LIMIT')) {
        const supabase = createClient(supabaseUrl, serviceKey);
        const runId = body.run_id as string;
        if (runId) {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId,
            p_level: 'ERROR',
            p_message: `WORKER_LIMIT in ${functionName}: HTTP ${resp.status}`,
            p_details: { step: body.step || functionName, chunk_index: body.chunk_index, http_status: resp.status, suggestion: 'ridurre CHUNK_LINES o ottimizzare chunking' }
          }).catch(() => {});
        }
      }
      
      return { success: false, error: `HTTP ${resp.status}: ${errorText}` };
    }
    
    const data = await resp.json().catch(() => ({ status: 'error', message: 'Invalid JSON response' }));
    if (data.status === 'error') {
      console.log(`[orchestrator] ${functionName} failed: ${data.message || data.error}`);
      return { success: false, error: data.message || data.error, data };
    }
    console.log(`[orchestrator] ${functionName} completed, step_status=${data.step_status || 'N/A'}`);
    return { success: true, data };
  } catch (e: unknown) {
    console.error(`[orchestrator] Error calling ${functionName}:`, e);
    return { success: false, error: errMsg(e) };
  }
}

async function verifyStepCompleted(supabase: SupabaseClient, runId: string, stepName: string): Promise<{ success: boolean; status?: string; error?: string }> {
  const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  const stepResult = run?.steps?.[stepName];
  
  if (!stepResult) {
    console.log(`[orchestrator] Step ${stepName} has no result in database - likely crashed`);
    return { success: false, error: `Step ${stepName} non ha prodotto risultati (possibile crash per memoria)` };
  }
  
  if (stepResult.status === 'failed') {
    console.log(`[orchestrator] Step ${stepName} failed: ${stepResult.error}`);
    return { success: false, status: 'failed', error: stepResult.error };
  }
  
  const intermediatePhases = ['building_stock_index', 'building_price_index', 'preparing_material', 'in_progress', 'finalizing'];
  if (intermediatePhases.includes(stepResult.status)) {
    console.log(`[orchestrator] Step ${stepName} is ${stepResult.status} (needs more invocations)`);
    return { success: true, status: 'in_progress' };
  }
  
  if (stepResult.status === 'completed' || stepResult.status === 'success') {
    console.log(`[orchestrator] Step ${stepName} verified as completed`);
    return { success: true, status: 'completed' };
  }
  
  if (stepResult.status === 'pending') {
    console.log(`[orchestrator] Step ${stepName} is pending (first invocation needed)`);
    return { success: true, status: 'in_progress' };
  }
  
  console.log(`[orchestrator] Step ${stepName} has unexpected status: ${stepResult.status}`);
  return { success: false, error: `Step ${stepName} stato imprevisto: ${stepResult.status}` };
}

async function isCancelRequested(supabase: SupabaseClient, runId: string): Promise<boolean> {
  const { data } = await supabase.from('sync_runs').select('cancel_requested').eq('id', runId).single();
  return data?.cancel_requested === true;
}

async function updateRun(supabase: SupabaseClient, runId: string, updates: Record<string, unknown>): Promise<void> {
  await supabase.from('sync_runs').update(updates).eq('id', runId);
}

async function finalizeRun(supabase: SupabaseClient, runId: string, status: string, startTime: number, errorMessage?: string): Promise<void> {
  await supabase.from('sync_runs').update({
    status, 
    finished_at: new Date().toISOString(), 
    runtime_ms: Date.now() - startTime,
    error_message: errorMessage || null
  }).eq('id', runId);
  console.log(`[orchestrator] Run ${runId} finalized: ${status}`);
}

serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });
  if (req.method !== 'POST') {
    return new Response(JSON.stringify({ status: 'error', message: 'Method not allowed' }), 
      { status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }

  const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
  const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
  const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY')!;
  
  let runId: string | null = null;
  let startTime = Date.now();
  let supabase: SupabaseClient | null = null;

  try {
    const body = await req.json().catch(() => ({}));
    const trigger = body.trigger as string;
    const attemptNumber = (body.attempt as number) || 1;
    
    if (!trigger || !['cron', 'manual'].includes(trigger)) {
      return new Response(JSON.stringify({ status: 'error', message: 'trigger deve essere cron o manual' }), 
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    console.log(`[orchestrator] Starting pipeline, trigger: ${trigger}`);

    if (trigger === 'manual') {
      const authHeader = req.headers.get('Authorization');
      if (!authHeader?.startsWith('Bearer ')) {
        return new Response(JSON.stringify({ status: 'error', message: 'Auth required' }), 
          { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      const jwt = authHeader.replace('Bearer ', '');
      const userClient = createClient(supabaseUrl, supabaseAnonKey, { global: { headers: { Authorization: `Bearer ${jwt}` } } });
      const { data: userData, error: userError } = await userClient.auth.getUser();
      if (userError || !userData?.user) {
        return new Response(JSON.stringify({ status: 'error', message: 'Invalid token' }), 
          { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      console.log(`[orchestrator] Manual trigger by user: ${userData.user.id}`);
    }

    supabase = createClient(supabaseUrl, supabaseServiceKey);

    // Load sync_config for run_timeout_minutes
    const { data: syncConfig } = await supabase.from('sync_config').select('run_timeout_minutes').eq('id', 1).single();
    const runTimeoutMinutes = syncConfig?.run_timeout_minutes || 60;

    // P1-B: Atomic lock acquisition BEFORE creating run
    runId = crypto.randomUUID();
    const LOCK_NAME = 'global_sync';
    const ttlSeconds = runTimeoutMinutes * 60;

    const { data: lockResult } = await supabase.rpc('try_acquire_sync_lock', {
      p_lock_name: LOCK_NAME,
      p_run_id: runId,
      p_ttl_seconds: ttlSeconds
    });

    if (!lockResult) {
      console.log('[orchestrator] INFO: Lock not acquired, sync already in progress');
      runId = null; // Don't release in finally
      return new Response(JSON.stringify({ status: 'locked', message: 'not_started' }), 
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    console.log(`[orchestrator] Lock acquired for run ${runId}`);

    // Use singleton row for deterministic fee_config read
    const FEE_CONFIG_SINGLETON_ID = '00000000-0000-0000-0000-000000000001';
    const { data: feeData } = await supabase.from('fee_config').select('*').eq('id', FEE_CONFIG_SINGLETON_ID).maybeSingle();
    const feeConfig = {
      feeDrev: feeData?.fee_drev ?? 1.05,
      feeMkt: feeData?.fee_mkt ?? 1.08,
      shippingCost: feeData?.shipping_cost ?? 6.00,
      mediaworldPrepDays: feeData?.mediaworld_preparation_days ?? 3,
      epricePrepDays: feeData?.eprice_preparation_days ?? 1,
      mediaworldIncludeEu: feeData?.mediaworld_include_eu ?? false,
      mediaworldItPrepDays: feeData?.mediaworld_it_preparation_days ?? 3,
      mediaworldEuPrepDays: feeData?.mediaworld_eu_preparation_days ?? 5,
      epriceIncludeEu: feeData?.eprice_include_eu ?? false,
      epriceItPrepDays: feeData?.eprice_it_preparation_days ?? 1,
      epriceEuPrepDays: feeData?.eprice_eu_preparation_days ?? 3,
      eanFeeDrev: feeData?.ean_fee_drev ?? null,
      eanFeeMkt: feeData?.ean_fee_mkt ?? null,
      eanShippingCost: feeData?.ean_shipping_cost ?? null,
      mediaworldFeeDrev: feeData?.mediaworld_fee_drev ?? null,
      mediaworldFeeMkt: feeData?.mediaworld_fee_mkt ?? null,
      mediaworldShippingCost: feeData?.mediaworld_shipping_cost ?? null,
      epriceFeeDrev: feeData?.eprice_fee_drev ?? null,
      epriceFeeMkt: feeData?.eprice_fee_mkt ?? null,
      epriceShippingCost: feeData?.eprice_shipping_cost ?? null
    };

    startTime = Date.now();
    await supabase.from('sync_runs').insert({ 
      id: runId, started_at: new Date().toISOString(), status: 'running', 
      trigger_type: trigger, attempt: attemptNumber, steps: { current_step: 'import_ftp' }, metrics: {},
      location_warnings: {}, warning_count: 0, file_manifest: {}
    });
    console.log(`[orchestrator] Run created: ${runId}`);

    // ========== STEP 1: FTP Import ==========
    await updateRun(supabase, runId, { steps: { current_step: 'import_ftp' } });
    console.log('[orchestrator] === STEP 1: FTP Import ===');
    
    for (const fileType of ['material', 'stock', 'price', 'stockLocation']) {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, 'Interrotta dall\'utente');
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'import-catalog-ftp', { 
        fileType,
        run_id: runId
      });
      
      if (!result.success && fileType !== 'stockLocation') {
        await finalizeRun(supabase, runId, 'failed', startTime, `FTP ${fileType}: ${result.error}`);
        return new Response(JSON.stringify({ status: 'failed', error: result.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      if (!result.success && fileType === 'stockLocation') {
        console.log(`[orchestrator] Stock location import failed (non-blocking): ${result.error}`);
        const { data: run } = await supabase.from('sync_runs').select('location_warnings').eq('id', runId).single();
        const warnings = run?.location_warnings || {};
        warnings.missing_location_file = 1;
        await supabase.from('sync_runs').update({ location_warnings: warnings }).eq('id', runId);
        
        // Register WARN via atomic RPC (increments warning_count)
        await supabase.rpc('log_sync_event', {
          p_run_id: runId,
          p_level: 'WARN',
          p_message: 'File stock location non trovato o non importabile',
          p_details: { step: 'import_ftp', location_warning: 'missing_location_file' }
        });
      }
    }

    // ========== STEP 2: PARSE_MERGE (CHUNKED) ==========
    await updateRun(supabase, runId, { steps: { current_step: 'parse_merge' } });
    console.log('[orchestrator] === STEP 2: parse_merge (CHUNKED) ===');
    
    let parseMergeComplete = false;
    let chunkCount = 0;
    
    while (!parseMergeComplete && chunkCount < MAX_PARSE_MERGE_CHUNKS) {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, 'Interrotta dall\'utente');
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      chunkCount++;
      console.log(`[orchestrator] parse_merge chunk ${chunkCount}/${MAX_PARSE_MERGE_CHUNKS}...`);
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
        run_id: runId, step: 'parse_merge', fee_config: feeConfig 
      });
      
      if (!result.success) {
        await finalizeRun(supabase, runId, 'failed', startTime, `parse_merge chunk ${chunkCount}: ${result.error}`);
        return new Response(JSON.stringify({ status: 'failed', error: result.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      const verification = await verifyStepCompleted(supabase, runId, 'parse_merge');
      
      if (!verification.success) {
        await finalizeRun(supabase, runId, 'failed', startTime, verification.error || 'parse_merge verification failed');
        return new Response(JSON.stringify({ status: 'failed', error: verification.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      if (verification.status === 'completed') {
        parseMergeComplete = true;
        console.log(`[orchestrator] parse_merge completed after ${chunkCount} chunks`);
      } else if (verification.status === 'in_progress') {
        console.log(`[orchestrator] parse_merge in_progress, continuing to next chunk...`);
      } else {
        await finalizeRun(supabase, runId, 'failed', startTime, `parse_merge unexpected status: ${verification.status}`);
        return new Response(JSON.stringify({ status: 'failed', error: `Unexpected status: ${verification.status}` }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }
    
    if (!parseMergeComplete) {
      await finalizeRun(supabase, runId, 'failed', startTime, `parse_merge exceeded ${MAX_PARSE_MERGE_CHUNKS} chunks limit`);
      return new Response(JSON.stringify({ status: 'failed', error: 'parse_merge chunk limit exceeded' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // ========== STEPS 3-7: Processing via sync-step-runner ==========
    const remainingSteps = ['ean_mapping', 'pricing', 'export_ean', 'export_mediaworld', 'export_eprice'];
    
    // For cron triggers, add EAN XLSX and Amazon export steps
    if (trigger === 'cron') {
      remainingSteps.push('export_ean_xlsx', 'export_amazon');
    }
    
    for (const step of remainingSteps) {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, 'Interrotta dall\'utente');
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      await updateRun(supabase, runId, { steps: { current_step: step } });
      console.log(`[orchestrator] === STEP: ${step} ===`);
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
        run_id: runId, step, fee_config: feeConfig 
      });
      
      const verification = await verifyStepCompleted(supabase, runId, step);
      
      if (!result.success || !verification.success) {
        const error = result.error || verification.error || `Step ${step} fallito`;
        await finalizeRun(supabase, runId, 'failed', startTime, error);
        return new Response(JSON.stringify({ status: 'failed', error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ========== STEP 8: SFTP Upload (ONLY for cron trigger) ==========
    if (trigger === 'cron') {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, 'Interrotta dall\'utente');
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      await updateRun(supabase, runId, { steps: { current_step: 'upload_sftp' } });
      console.log('[orchestrator] === STEP 8: SFTP Upload (cron only) ===');
      
      // Exactly the 5 required files, same remote folder, fixed names, overwrite
      const sftpFiles = EXPORT_FILES.map(f => ({
        bucket: 'exports',
        path: f,
        filename: f
      }));
      
      const sftpResult = await callStep(supabaseUrl, supabaseServiceKey, 'upload-exports-to-sftp', {
        files: sftpFiles
      });
      
      if (!sftpResult.success) {
        await finalizeRun(supabase, runId, 'failed', startTime, `SFTP: ${sftpResult.error}`);
        return new Response(JSON.stringify({ status: 'failed', error: sftpResult.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    } else {
      console.log('[orchestrator] Skipping SFTP upload (manual trigger)');
    }

    // ========== STORAGE VERSIONING ==========
    console.log('[orchestrator] === Storage versioning ===');
    const versionTimestamp = new Date().toISOString().replace(/[:.]/g, '-');
    
    // Save file_manifest with version timestamp for retention tracking
    const fileManifest: Record<string, string> = {};
    
    for (const fileName of EXPORT_FILES) {
      try {
        const { data: fileBlob } = await supabase.storage.from('exports').download(fileName);
        if (fileBlob) {
          // Copy to latest/ (overwrite)
          await supabase.storage.from('exports').upload(`latest/${fileName}`, fileBlob, { upsert: true });
          // Copy to versions/
          await supabase.storage.from('exports').upload(`versions/${versionTimestamp}/${fileName}`, fileBlob, { upsert: true });
          fileManifest[fileName] = versionTimestamp;
        }
      } catch (e: unknown) {
        console.warn(`[orchestrator] Versioning failed for ${fileName}: ${errMsg(e)}`);
      }
    }
    
    // Save file_manifest for retention tracking
    await supabase.from('sync_runs').update({ file_manifest: fileManifest }).eq('id', runId);

    // ========== RETENTION CLEANUP (per-file, max 3 versions, >7 days) ==========
    await cleanupVersions(supabase);

    // ========== DETERMINE FINAL STATUS ==========
    // success_with_warning based SOLELY on warning_count > 0
    const { data: finalRun } = await supabase.from('sync_runs').select('warning_count').eq('id', runId).single();
    const finalStatus = (finalRun?.warning_count || 0) > 0 ? 'success_with_warning' : 'success';
    
    await finalizeRun(supabase, runId, finalStatus, startTime);
    console.log(`[orchestrator] Pipeline completed: ${runId}, status: ${finalStatus}`);

    // ========== POST-RUN NOTIFICATION ==========
    try {
      await fetch(`${supabaseUrl}/functions/v1/send-sync-notification`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${supabaseServiceKey}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ run_id: runId, status: finalStatus })
      });
      console.log(`[orchestrator] Notification sent for run ${runId}`);
    } catch (e: unknown) {
      console.warn(`[orchestrator] Notification failed (non-blocking): ${errMsg(e)}`);
    }
    
    return new Response(JSON.stringify({ status: finalStatus, run_id: runId }), 
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

  } catch (err: unknown) {
    console.error('[orchestrator] Fatal error:', err);
    
    if (runId && supabase) {
      try {
        await finalizeRun(supabase, runId, 'failed', startTime, errMsg(err));
        await fetch(`${supabaseUrl}/functions/v1/send-sync-notification`, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${supabaseServiceKey}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ run_id: runId, status: 'failed' })
        }).catch(() => {});
      } catch (e: unknown) {
        console.error('[orchestrator] Failed to finalize:', e);
      }
    }
    
    return new Response(JSON.stringify({ status: 'error', message: errMsg(err) }), 
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  } finally {
    // P1-B: Always release lock in finally
    if (runId && supabase) {
      try {
        const { data: released } = await supabase.rpc('release_sync_lock', {
          p_lock_name: 'global_sync',
          p_run_id: runId
        });
        if (released) {
          console.log(`[orchestrator] Lock released for run ${runId}`);
        } else {
          console.warn(`[orchestrator] WARN: Lock release returned false for run ${runId} (run_id mismatch or already released)`);
        }
      } catch (e: unknown) {
        console.warn(`[orchestrator] WARN: Failed to release lock: ${errMsg(e)}`);
      }
    }
  }
});

/**
 * Per-file retention cleanup.
 * For each of the 5 canonical files:
 * - Find all versions under versions/<timestamp>/<filename>
 * - Keep the newest 3 versions
 * - Delete versions beyond 3 only if older than 7 days
 * - Never delete latest/
 */
async function cleanupVersions(supabase: SupabaseClient): Promise<void> {
  try {
    const { data: versionFolders } = await supabase.storage.from('exports').list('versions', {
      sortBy: { column: 'name', order: 'desc' },
      limit: 200
    });

    if (!versionFolders || versionFolders.length === 0) {
      console.log('[orchestrator] No version folders found, skipping cleanup');
      return;
    }

    const sevenDaysAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;

    // Build per-file version list
    const fileVersions: Record<string, Array<{ folder: string; timestamp: number }>> = {};
    for (const f of EXPORT_FILES) {
      fileVersions[f] = [];
    }

    for (const folder of versionFolders) {
      const folderName = folder.name;
      // Parse timestamp from folder name (format: YYYY-MM-DDTHH-MM-SS-mmmZ)
      let folderTs: number;
      try {
        const isoStr = folderName.replace(/-(\d{2})-(\d{2})-(\d{3})Z$/, ':$1:$2.$3Z').replace(/T(\d{2})-/, 'T$1:');
        folderTs = new Date(isoStr).getTime();
        if (isNaN(folderTs)) {
          // Fallback: use folder metadata created_at if available
          folderTs = folder.created_at ? new Date(folder.created_at).getTime() : 0;
        }
      } catch {
        folderTs = folder.created_at ? new Date(folder.created_at).getTime() : 0;
      }

      if (folderTs === 0) {
        console.warn(`[orchestrator] WARN: Cannot determine timestamp for version folder: ${folderName}, skipping`);
        continue;
      }

      // List files in this version folder
      const { data: filesInFolder } = await supabase.storage.from('exports').list(`versions/${folderName}`);
      if (!filesInFolder) continue;

      for (const file of filesInFolder) {
        if (EXPORT_FILES.includes(file.name)) {
          fileVersions[file.name].push({ folder: folderName, timestamp: folderTs });
        }
      }
    }

    // For each file, apply retention
    const toDelete: string[] = [];

    for (const [fileName, versions] of Object.entries(fileVersions)) {
      // Sort newest first
      versions.sort((a, b) => b.timestamp - a.timestamp);
      
      if (versions.length <= 3) continue;

      // Versions beyond the first 3
      const excess = versions.slice(3);
      for (const v of excess) {
        if (v.timestamp < sevenDaysAgo) {
          toDelete.push(`versions/${v.folder}/${fileName}`);
        }
      }
    }

    if (toDelete.length > 0) {
      console.log(`[orchestrator] Deleting ${toDelete.length} old version files`);
      // Delete in batches of 20
      for (let i = 0; i < toDelete.length; i += 20) {
        const batch = toDelete.slice(i, i + 20);
        await supabase.storage.from('exports').remove(batch);
      }
      console.log(`[orchestrator] Retention cleanup completed`);
    } else {
      console.log('[orchestrator] No version files to clean up');
    }

    // Clean up empty version folders
    try {
      for (const folder of versionFolders) {
        const { data: remaining } = await supabase.storage.from('exports').list(`versions/${folder.name}`);
        if (!remaining || remaining.length === 0) {
          // Folder is empty, but Supabase storage doesn't have explicit folder deletion
          // Empty folders are cleaned up automatically
        }
      }
    } catch (e: unknown) {
      console.warn(`[orchestrator] Folder cleanup warning: ${errMsg(e)}`);
    }
  } catch (e: unknown) {
    console.warn(`[orchestrator] Version cleanup failed (non-blocking): ${errMsg(e)}`);
  }
}
