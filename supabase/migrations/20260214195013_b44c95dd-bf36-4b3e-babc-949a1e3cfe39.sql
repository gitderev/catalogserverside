
-- Tighten sync_events insert policy: only admins can insert via client
-- (Edge functions use service role which bypasses RLS anyway)
DROP POLICY IF EXISTS "sync_events_insert_service" ON public.sync_events;

CREATE POLICY "sync_events_insert_admin"
  ON public.sync_events
  FOR INSERT
  WITH CHECK (EXISTS (
    SELECT 1 FROM admin_users au WHERE au.user_id = auth.uid()
  ));

-- Also add DELETE policy for admin cleanup
CREATE POLICY "sync_events_delete_admin"
  ON public.sync_events
  FOR DELETE
  USING (EXISTS (
    SELECT 1 FROM admin_users au WHERE au.user_id = auth.uid()
  ));
