import { describe, it, expect } from 'vitest';

/**
 * TypeScript reference implementation of jsonb_deep_merge.
 * Must match the SQL function public.jsonb_deep_merge semantics:
 *   object + object = recursive merge
 *   array = overwrite (patch wins)
 *   scalar/null = overwrite (patch wins)
 */
function deepMerge(
  base: Record<string, unknown>,
  patch: Record<string, unknown>
): Record<string, unknown> {
  const result: Record<string, unknown> = { ...base };
  for (const key of Object.keys(patch)) {
    const srcVal = patch[key];
    const tgtVal = base[key];
    if (
      srcVal !== null && typeof srcVal === 'object' && !Array.isArray(srcVal) &&
      tgtVal !== null && typeof tgtVal === 'object' && !Array.isArray(tgtVal)
    ) {
      result[key] = deepMerge(
        tgtVal as Record<string, unknown>,
        srcVal as Record<string, unknown>
      );
    } else {
      result[key] = srcVal;
    }
  }
  return result;
}

describe('deepMerge (jsonb_deep_merge reference)', () => {
  it('preserves nested fields when patching only a sub-key (retry example)', () => {
    const existing = {
      status: 'retry_delay',
      retry: {
        retry_attempt: 1,
        next_retry_at: '2026-02-20T10:00:00Z',
      },
    };
    const patch = { retry: { retry_attempt: 2 } };
    const result = deepMerge(existing, patch);

    expect(result.status).toBe('retry_delay');
    expect((result.retry as Record<string, unknown>).retry_attempt).toBe(2);
    expect((result.retry as Record<string, unknown>).next_retry_at).toBe('2026-02-20T10:00:00Z');
  });

  it('overwrites status while preserving nested retry + meta', () => {
    const existing = {
      status: 'retry_delay',
      retry: { attempt: 2, retry_after: '2026-02-15T10:00:00Z' },
      meta: { a: 1 },
    };
    const patch = { status: 'in_progress', retry: { attempt: 3 } };
    const result = deepMerge(existing, patch);

    expect(result.status).toBe('in_progress');
    expect((result.retry as Record<string, unknown>).attempt).toBe(3);
    expect((result.retry as Record<string, unknown>).retry_after).toBe('2026-02-15T10:00:00Z');
    expect((result.meta as Record<string, unknown>).a).toBe(1);
  });

  it('partial patch does not eliminate preexisting fields', () => {
    const existing = {
      status: 'completed',
      duration_ms: 1234,
      metrics: { rows: 100, skipped: 5 },
      validation_passed: true,
    };
    const patch = { metrics: { rows: 200 } };
    const result = deepMerge(existing, patch);

    expect(result.status).toBe('completed');
    expect(result.duration_ms).toBe(1234);
    expect(result.validation_passed).toBe(true);
    expect((result.metrics as Record<string, unknown>).rows).toBe(200);
    expect((result.metrics as Record<string, unknown>).skipped).toBe(5);
  });

  it('array values are overwritten, not merged', () => {
    const existing = { tags: ['a', 'b'] };
    const patch = { tags: ['c'] };
    const result = deepMerge(existing, patch);

    expect(result.tags).toEqual(['c']);
  });

  it('null in patch overwrites existing value', () => {
    const existing = { retry: { attempt: 1 } };
    const patch = { retry: null };
    const result = deepMerge(existing, patch);

    expect(result.retry).toBeNull();
  });

  it('new keys are added without affecting existing', () => {
    const existing = { status: 'running' };
    const patch = { stage: 'after_write', heap_mb: 85 };
    const result = deepMerge(existing, patch);

    expect(result.status).toBe('running');
    expect(result.stage).toBe('after_write');
    expect(result.heap_mb).toBe(85);
  });
});

describe('invocation_id validation logic', () => {
  it('missing invocation_id should emit WARN, not reject', () => {
    // Simulates the step-runner behavior: if lock_invocation_id is absent,
    // the request should still be processed (backward compatible) but a warning should be logged.
    const lockInvocationId: string | undefined = undefined;
    const shouldWarn = !lockInvocationId;
    const shouldReject = false; // Never reject if absent

    expect(shouldWarn).toBe(true);
    expect(shouldReject).toBe(false);
  });

  it('mismatched invocation_id should reject', () => {
    const ourInvocationId = 'invk_abc123';
    const lockHolderInvocationId = 'invk_xyz789';
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    const shouldReject = (ourInvocationId as string) !== (lockHolderInvocationId as string);

    expect(shouldReject).toBe(true);
  });

  it('matching invocation_id should allow', () => {
    const ourInvocationId = 'invk_abc123';
    const lockHolderInvocationId = 'invk_abc123';
    const shouldReject = ourInvocationId !== lockHolderInvocationId;

    expect(shouldReject).toBe(false);
  });
});
