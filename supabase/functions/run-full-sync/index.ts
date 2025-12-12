import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

/**
 * run-full-sync - TICK-BASED ORCHESTRATOR
 * 
 * Each invocation performs at most ONE unit of work:
 * - Acquire lock or exit if locked
 * - Resume existing running run OR create new run
 * - Execute ONE step (or ONE chunk of parse_merge)
 * - Release lock and return
 */

const ALL_STEPS = ['import_ftp', 'parse_merge', 'ean_mapping', 'pricing', 'export_ean', 'export_mediaworld', 'export_eprice', 'upload_sftp'];
const LOCK_KEY = 'pipeline';
const LOCK_TTL_SECONDS = 120;

// ========== LOCK MANAGEMENT ==========
async function acquireLock(supabase: any, lockId: string): Promise<{ acquired: boolean; existingRunId?: string }> {
  const now = new Date();
  const expiresAt = new Date(now.getTime() + LOCK_TTL_SECONDS * 1000);
  
  const { data: existing } = await supabase.from('sync_locks').select('*').eq('lock_key', LOCK_KEY).maybeSingle();
  
  if (existing) {
    const existingExpiry = new Date(existing.expires_at);
    if (existingExpiry > now) {
      console.log(`[orchestrator] Lock held by ${existing.locked_by}, expires ${existing.expires_at}`);
      return { acquired: false, existingRunId: existing.run_id };
    }
  }
  
  const { error } = await supabase.from('sync_locks').upsert({
    lock_key: LOCK_KEY,
    locked_by: lockId,
    locked_at: now.toISOString(),
    expires_at: expiresAt.toISOString(),
    run_id: null
  }, { onConflict: 'lock_key' });
  
  if (error) {
    console.error(`[orchestrator] Failed to acquire lock:`, error);
    return { acquired: false };
  }
  
  return { acquired: true };
}

async function updateLockRunId(supabase: any, runId: string): Promise<void> {
  await supabase.from('sync_locks').update({ 
    run_id: runId, 
    expires_at: new Date(Date.now() + LOCK_TTL_SECONDS * 1000).toISOString() 
  }).eq('lock_key', LOCK_KEY);
}

async function releaseLock(supabase: any): Promise<void> {
  await supabase.from('sync_locks').delete().eq('lock_key', LOCK_KEY);
  console.log(`[orchestrator] Lock released`);
}

// ========== STEP HELPERS ==========
async function callStep(supabaseUrl: string, serviceKey: string, functionName: string, body: any): Promise<{ success: boolean; error?: string; data?: any; httpStatus?: number }> {
  try {
    console.log(`[orchestrator] Calling ${functionName}...`);
    const resp = await fetch(`${supabaseUrl}/functions/v1/${functionName}`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    
    const httpStatus = resp.status;
    
    if (!resp.ok) {
      const errorText = await resp.text().catch(() => 'Unknown error');
      console.log(`[orchestrator] ${functionName} HTTP error ${resp.status}: ${errorText.substring(0, 200)}`);
      return { success: false, error: `HTTP ${resp.status}: ${errorText.substring(0, 200)}`, httpStatus };
    }
    
    const data = await resp.json().catch(() => ({ status: 'error', message: 'Invalid JSON' }));
    if (data.status === 'error') {
      return { success: false, error: data.message || data.error, data, httpStatus };
    }
    return { success: true, data, httpStatus };
  } catch (e: any) {
    return { success: false, error: e.message };
  }
}

async function updateRunSteps(supabase: any, runId: string, partialSteps: Record<string, any>): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  const existingSteps = run?.steps || {};
  
  const merged = { ...existingSteps };
  for (const [key, value] of Object.entries(partialSteps)) {
    if (key === 'exports' && typeof value === 'object' && typeof merged.exports === 'object') {
      merged.exports = { ...merged.exports, ...value, files: { ...(merged.exports?.files || {}), ...(value as any)?.files } };
    } else if (typeof value === 'object' && typeof merged[key] === 'object' && !Array.isArray(value)) {
      merged[key] = { ...merged[key], ...value };
    } else {
      merged[key] = value;
    }
  }
  
  await supabase.from('sync_runs').update({ steps: merged }).eq('id', runId);
}

async function finalizeRun(supabase: any, runId: string, status: string, startTime: number, errorMessage?: string, errorDetails?: any): Promise<void> {
  const updates: any = {
    status,
    finished_at: new Date().toISOString(),
    runtime_ms: Date.now() - startTime,
    error_message: errorMessage || null
  };
  if (errorDetails) updates.error_details = errorDetails;
  await supabase.from('sync_runs').update(updates).eq('id', runId);
}

// ========== BACKOFF ==========
function calculateBackoffDelay(retryCount: number): number {
  const baseDelayMs = 60000;
  const multiplier = 2;
  const maxDelayMs = 600000;
  const jitter = 0.2;
  
  let delay = baseDelayMs * Math.pow(multiplier, retryCount);
  delay = Math.min(delay, maxDelayMs);
  delay = Math.round(delay + delay * jitter * (Math.random() * 2 - 1));
  return delay;
}

function isRetryableError(error: string, httpStatus?: number): boolean {
  if (httpStatus === 546 || error.includes('WORKER_LIMIT') || error.includes('546')) return true;
  if (error.includes('timeout') || error.includes('Timeout')) return true;
  return false;
}

// ========== MAIN HANDLER ==========
serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });
  if (req.method !== 'POST') {
    return new Response(JSON.stringify({ status: 'error', message: 'Method not allowed' }), 
      { status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }

  const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
  const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
  const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY')!;
  const lockId = `orchestrator-${Date.now()}-${Math.random().toString(36).substring(7)}`;
  
  let supabase: any = null;
  let runId: string | null = null;
  let startTime = Date.now();
  let lockAcquired = false;

  try {
    const body = await req.json().catch(() => ({}));
    const trigger = body.trigger as string;
    
    if (!trigger || !['cron', 'manual'].includes(trigger)) {
      return new Response(JSON.stringify({ status: 'error', message: 'trigger deve essere cron o manual' }), 
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    console.log(`[orchestrator] Starting, trigger: ${trigger}`);

    // Authenticate manual triggers
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
    }

    supabase = createClient(supabaseUrl, supabaseServiceKey);

    // Acquire lock
    const lockResult = await acquireLock(supabase, lockId);
    if (!lockResult.acquired) {
      if (lockResult.existingRunId && trigger === 'cron') {
        return new Response(JSON.stringify({ status: 'busy', message: 'Pipeline già in corso', run_id: lockResult.existingRunId }), 
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      return new Response(JSON.stringify({ status: 'error', message: 'Pipeline lock held' }), 
        { status: 409, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    lockAcquired = true;

    // Check for existing running run
    const { data: runningRuns } = await supabase.from('sync_runs').select('*').eq('status', 'running').order('started_at', { ascending: false }).limit(1);
    
    let run: any = null;
    
    if (runningRuns?.length) {
      run = runningRuns[0];
      runId = run.id;
      startTime = new Date(run.started_at).getTime();
      console.log(`[orchestrator] Resuming run: ${runId}, current_step: ${run.steps?.current_step}`);
    } else {
      // Check sync_config for cron
      if (trigger === 'cron') {
        const { data: config } = await supabase.from('sync_config').select('enabled, frequency_minutes').eq('id', 1).single();
        if (!config?.enabled) {
          await releaseLock(supabase);
          return new Response(JSON.stringify({ status: 'skipped', message: 'Disabled' }), 
            { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        
        const { data: lastRun } = await supabase.from('sync_runs').select('started_at').eq('trigger_type', 'cron').eq('attempt', 1).order('started_at', { ascending: false }).limit(1);
        if (lastRun?.length) {
          const elapsed = Date.now() - new Date(lastRun[0].started_at).getTime();
          if (elapsed < config.frequency_minutes * 60 * 1000) {
            await releaseLock(supabase);
            return new Response(JSON.stringify({ status: 'skipped', message: 'Frequency not elapsed' }), 
              { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
          }
        }
      }

      // Validate fee_config
      const { data: feeData, error: feeError } = await supabase.from('fee_config').select('*').limit(1).single();
      if (feeError || !feeData) {
        await releaseLock(supabase);
        return new Response(JSON.stringify({ status: 'error', message: 'Configurazione fee_config mancante' }), 
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }

      // Create new run
      runId = crypto.randomUUID();
      startTime = Date.now();
      
      const initialSteps: Record<string, any> = { current_step: 'import_ftp' };
      for (const step of ALL_STEPS) initialSteps[step] = { status: 'pending' };
      
      await supabase.from('sync_runs').insert({ 
        id: runId, started_at: new Date().toISOString(), status: 'running', trigger_type: trigger, attempt: 1,
        steps: initialSteps, metrics: {}, location_warnings: {}, error_details: null
      });
      
      console.log(`[orchestrator] New run created: ${runId}`);
    }
    
    await updateLockRunId(supabase, runId!);
    
    // Get fresh run data
    const { data: currentRun } = await supabase.from('sync_runs').select('*').eq('id', runId).single();
    run = currentRun;
    
    if (run.cancel_requested) {
      await finalizeRun(supabase, runId!, 'cancelled', startTime, 'Interrotta dall\'utente');
      await releaseLock(supabase);
      return new Response(JSON.stringify({ status: 'cancelled', run_id: runId }), 
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    // Get fee config
    const { data: feeData } = await supabase.from('fee_config').select('*').limit(1).single();
    const feeConfig = feeData ? {
      feeDrev: feeData.fee_drev ?? 1.05, feeMkt: feeData.fee_mkt ?? 1.08, shippingCost: feeData.shipping_cost ?? 6.00,
      mediaworldIncludeEu: feeData.mediaworld_include_eu, mediaworldItPrepDays: feeData.mediaworld_it_preparation_days, mediaworldEuPrepDays: feeData.mediaworld_eu_preparation_days,
      epriceIncludeEu: feeData.eprice_include_eu, epriceItPrepDays: feeData.eprice_it_preparation_days, epriceEuPrepDays: feeData.eprice_eu_preparation_days
    } : {};
    
    const steps = run.steps || {};
    const currentStep = steps.current_step || 'import_ftp';
    
    console.log(`[orchestrator] Current step: ${currentStep}`);
    
    // Check waiting_retry
    const stepState = steps[currentStep];
    if (stepState?.phase === 'waiting_retry' && stepState?.next_retry_at) {
      const nextRetry = new Date(stepState.next_retry_at);
      if (nextRetry > new Date()) {
        console.log(`[orchestrator] Waiting retry until ${nextRetry.toISOString()}`);
        await releaseLock(supabase);
        return new Response(JSON.stringify({ status: 'waiting_retry', run_id: runId, next_retry_at: stepState.next_retry_at }), 
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }
    
    let stepResult: { success: boolean; error?: string; data?: any; httpStatus?: number };
    
    // ========== IMPORT_FTP ==========
    if (currentStep === 'import_ftp') {
      const importPhase = stepState?.importPhase || 'material';
      const fileTypes = ['material', 'stock', 'price', 'stockLocation'];
      const currentFileIndex = fileTypes.indexOf(importPhase);
      
      if (currentFileIndex < fileTypes.length) {
        const fileType = fileTypes[currentFileIndex];
        stepResult = await callStep(supabaseUrl, supabaseServiceKey, 'import-catalog-ftp', { fileType, run_id: runId });
        
        if (!stepResult.success && fileType !== 'stockLocation') {
          const errorDetails = { step: 'import_ftp', fileType, error: stepResult.error };
          await updateRunSteps(supabase, runId!, { import_ftp: { status: 'failed', error: stepResult.error, details: errorDetails } });
          await finalizeRun(supabase, runId!, 'failed', startTime, `FTP ${fileType}: ${stepResult.error}`, errorDetails);
          await releaseLock(supabase);
          return new Response(JSON.stringify({ status: 'failed', run_id: runId, error: stepResult.error }), 
            { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        
        if (!stepResult.success && fileType === 'stockLocation') {
          console.log(`[orchestrator] Stock location import failed (non-blocking): ${stepResult.error}`);
          const { data: runData } = await supabase.from('sync_runs').select('location_warnings').eq('id', runId).single();
          const warnings = runData?.location_warnings || {};
          warnings.missing_location_file = 1;
          await supabase.from('sync_runs').update({ location_warnings: warnings }).eq('id', runId);
        }
        
        // Save input file path (DETERMINISTIC)
        const inputPath = stepResult.data?.path;
        if (inputPath) {
          const inputFiles = steps.import_ftp?.details?.input_files || {};
          inputFiles[`${fileType}Path`] = inputPath;
          await updateRunSteps(supabase, runId!, { 
            import_ftp: { 
              ...steps.import_ftp,
              status: 'running',
              importPhase: fileTypes[currentFileIndex + 1] || 'done',
              details: { ...steps.import_ftp?.details, input_files: inputFiles }
            } 
          });
        } else {
          await updateRunSteps(supabase, runId!, { 
            import_ftp: { ...steps.import_ftp, importPhase: fileTypes[currentFileIndex + 1] || 'done' } 
          });
        }
        
        if (currentFileIndex + 1 >= fileTypes.length) {
          await updateRunSteps(supabase, runId!, { import_ftp: { ...steps.import_ftp, status: 'completed' }, current_step: 'parse_merge' });
        }
      } else {
        await updateRunSteps(supabase, runId!, { import_ftp: { status: 'completed' }, current_step: 'parse_merge' });
      }
      
    // ========== PARSE_MERGE ==========
    } else if (currentStep === 'parse_merge') {
      stepResult = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { run_id: runId, step: 'parse_merge', fee_config: feeConfig });
      
      if (!stepResult.success) {
        if (isRetryableError(stepResult.error || '', stepResult.httpStatus)) {
          const retryCount = (stepState?.retry_count || 0) + 1;
          if (retryCount > 10) {
            const errorDetails = { step: 'parse_merge', retry_count: retryCount, error: stepResult.error };
            await updateRunSteps(supabase, runId!, { parse_merge: { status: 'failed', error: 'Max retries exceeded', details: errorDetails } });
            await finalizeRun(supabase, runId!, 'failed', startTime, 'parse_merge: max retries exceeded', errorDetails);
            await releaseLock(supabase);
            return new Response(JSON.stringify({ status: 'failed', run_id: runId }), 
              { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
          }
          
          const delay = calculateBackoffDelay(retryCount);
          const nextRetry = new Date(Date.now() + delay);
          
          await updateRunSteps(supabase, runId!, { 
            parse_merge: { 
              ...steps.parse_merge,
              phase: 'waiting_retry',
              retry_count: retryCount,
              next_retry_at: nextRetry.toISOString(),
              last_error: stepResult.error,
              last_error_at: new Date().toISOString()
            } 
          });
          
          await releaseLock(supabase);
          return new Response(JSON.stringify({ status: 'waiting_retry', run_id: runId, retry_count: retryCount, next_retry_at: nextRetry.toISOString() }), 
            { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        
        const errorDetails = { step: 'parse_merge', error: stepResult.error };
        await updateRunSteps(supabase, runId!, { parse_merge: { status: 'failed', error: stepResult.error, details: errorDetails } });
        await finalizeRun(supabase, runId!, 'failed', startTime, `parse_merge: ${stepResult.error}`, errorDetails);
        await releaseLock(supabase);
        return new Response(JSON.stringify({ status: 'failed', run_id: runId }), 
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      const { data: updatedRun } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
      const pmState = updatedRun?.steps?.parse_merge;
      
      if (pmState?.status === 'completed' || pmState?.phase === 'completed') {
        await updateRunSteps(supabase, runId!, { 
          parse_merge: { ...pmState, retry_count: 0, next_retry_at: null, last_error: null },
          current_step: 'ean_mapping' 
        });
      } else if (pmState?.productCount > (stepState?.productCount || 0) || pmState?.offsetBytes > (stepState?.offsetBytes || 0)) {
        await updateRunSteps(supabase, runId!, { 
          parse_merge: { ...pmState, retry_count: 0, next_retry_at: null, last_error: null }
        });
      }
      
    // ========== UPLOAD_SFTP ==========
    } else if (currentStep === 'upload_sftp') {
      const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
      const exportFiles = runData?.steps?.exports?.files || {};
      
      const missingPaths: string[] = [];
      if (!exportFiles.ean) missingPaths.push('ean');
      if (!exportFiles.mediaworld) missingPaths.push('mediaworld');
      if (!exportFiles.eprice) missingPaths.push('eprice');
      
      if (missingPaths.length > 0) {
        const error = `Missing export paths: ${missingPaths.join(', ')}`;
        const errorDetails = { step: 'upload_sftp', missing_paths: missingPaths, available_paths: exportFiles };
        await updateRunSteps(supabase, runId!, { upload_sftp: { status: 'failed', error, details: errorDetails } });
        await finalizeRun(supabase, runId!, 'failed', startTime, error, errorDetails);
        await releaseLock(supabase);
        return new Response(JSON.stringify({ status: 'failed', run_id: runId, error }), 
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      const files = [
        { bucket: 'exports', path: exportFiles.ean.replace(/^exports\//, ''), filename: 'Catalogo EAN.xlsx' },
        { bucket: 'exports', path: exportFiles.mediaworld.replace(/^exports\//, ''), filename: 'Export Mediaworld.xlsx' },
        { bucket: 'exports', path: exportFiles.eprice.replace(/^exports\//, ''), filename: 'Export ePrice.xlsx' }
      ];
      
      stepResult = await callStep(supabaseUrl, supabaseServiceKey, 'upload-exports-to-sftp', { files });
      
      if (!stepResult.success) {
        const errorDetails = { step: 'upload_sftp', files, error: stepResult.error };
        await updateRunSteps(supabase, runId!, { upload_sftp: { status: 'failed', error: stepResult.error, details: errorDetails } });
        await finalizeRun(supabase, runId!, 'failed', startTime, `SFTP: ${stepResult.error}`, errorDetails);
        await releaseLock(supabase);
        return new Response(JSON.stringify({ status: 'failed', run_id: runId }), 
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      await updateRunSteps(supabase, runId!, { upload_sftp: { status: 'completed' } });
      await finalizeRun(supabase, runId!, 'success', startTime);
      await releaseLock(supabase);
      return new Response(JSON.stringify({ status: 'success', run_id: runId }), 
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      
    // ========== OTHER STEPS ==========
    } else {
      stepResult = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { run_id: runId, step: currentStep, fee_config: feeConfig });
      
      if (!stepResult.success) {
        const errorDetails = { step: currentStep, error: stepResult.error };
        await updateRunSteps(supabase, runId!, { [currentStep]: { status: 'failed', error: stepResult.error, details: errorDetails } });
        await finalizeRun(supabase, runId!, 'failed', startTime, `${currentStep}: ${stepResult.error}`, errorDetails);
        await releaseLock(supabase);
        return new Response(JSON.stringify({ status: 'failed', run_id: runId }), 
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      // Move to next step
      const stepIndex = ALL_STEPS.indexOf(currentStep);
      if (stepIndex < ALL_STEPS.length - 1) {
        await updateRunSteps(supabase, runId!, { [currentStep]: { status: 'completed' }, current_step: ALL_STEPS[stepIndex + 1] });
      }
    }
    
    await releaseLock(supabase);
    
    return new Response(JSON.stringify({ status: 'running', run_id: runId, current_step: currentStep, message: 'Step eseguito, pipeline in corso' }), 
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

  } catch (err: any) {
    console.error('[orchestrator] Fatal error:', err);
    
    if (runId && supabase) {
      try {
        const errorDetails = { fatal: true, exception: err.message, stack: err.stack?.substring(0, 500) };
        await finalizeRun(supabase, runId, 'failed', startTime, err.message, errorDetails);
      } catch (_) {}
    }
    
    if (lockAcquired && supabase) {
      try { await releaseLock(supabase); } catch (_) {}
    }
    
    return new Response(JSON.stringify({ error: err.message, details: 'Errore pipeline', run_id: runId || null }), 
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }
});
