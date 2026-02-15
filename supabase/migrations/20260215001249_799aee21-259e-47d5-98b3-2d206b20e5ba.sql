-- Ensure sync_config singleton row exists (id=1)
INSERT INTO sync_config (id, enabled, frequency_minutes, schedule_type, notification_mode, notify_on_warning, retry_delay_minutes, max_retries, run_timeout_minutes, max_attempts)
VALUES (1, false, 60, 'hours', 'only_on_problem', true, 5, 5, 60, 3)
ON CONFLICT (id) DO NOTHING;