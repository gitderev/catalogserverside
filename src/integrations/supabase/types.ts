export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  // Allows to automatically instantiate createClient with right options
  // instead of createClient<Database, { PostgrestVersion: 'XX' }>(URL, KEY)
  __InternalSupabase: {
    PostgrestVersion: "14.1"
  }
  public: {
    Tables: {
      admin_users: {
        Row: {
          created_at: string | null
          user_id: string
        }
        Insert: {
          created_at?: string | null
          user_id: string
        }
        Update: {
          created_at?: string | null
          user_id?: string
        }
        Relationships: []
      }
      fee_config: {
        Row: {
          amazon_eu_preparation_days: number
          amazon_fee_drev: number | null
          amazon_fee_mkt: number | null
          amazon_include_eu: boolean
          amazon_it_preparation_days: number
          amazon_shipping_cost: number | null
          created_at: string | null
          ean_fee_drev: number | null
          ean_fee_mkt: number | null
          ean_shipping_cost: number | null
          eprice_eu_preparation_days: number
          eprice_fee_drev: number | null
          eprice_fee_mkt: number | null
          eprice_include_eu: boolean
          eprice_it_preparation_days: number
          eprice_preparation_days: number
          eprice_shipping_cost: number | null
          fee_drev: number
          fee_mkt: number
          id: string
          mediaworld_eu_preparation_days: number
          mediaworld_fee_drev: number | null
          mediaworld_fee_mkt: number | null
          mediaworld_include_eu: boolean
          mediaworld_it_preparation_days: number
          mediaworld_preparation_days: number
          mediaworld_shipping_cost: number | null
          shipping_cost: number
          updated_at: string | null
        }
        Insert: {
          amazon_eu_preparation_days?: number
          amazon_fee_drev?: number | null
          amazon_fee_mkt?: number | null
          amazon_include_eu?: boolean
          amazon_it_preparation_days?: number
          amazon_shipping_cost?: number | null
          created_at?: string | null
          ean_fee_drev?: number | null
          ean_fee_mkt?: number | null
          ean_shipping_cost?: number | null
          eprice_eu_preparation_days?: number
          eprice_fee_drev?: number | null
          eprice_fee_mkt?: number | null
          eprice_include_eu?: boolean
          eprice_it_preparation_days?: number
          eprice_preparation_days?: number
          eprice_shipping_cost?: number | null
          fee_drev: number
          fee_mkt: number
          id?: string
          mediaworld_eu_preparation_days?: number
          mediaworld_fee_drev?: number | null
          mediaworld_fee_mkt?: number | null
          mediaworld_include_eu?: boolean
          mediaworld_it_preparation_days?: number
          mediaworld_preparation_days?: number
          mediaworld_shipping_cost?: number | null
          shipping_cost: number
          updated_at?: string | null
        }
        Update: {
          amazon_eu_preparation_days?: number
          amazon_fee_drev?: number | null
          amazon_fee_mkt?: number | null
          amazon_include_eu?: boolean
          amazon_it_preparation_days?: number
          amazon_shipping_cost?: number | null
          created_at?: string | null
          ean_fee_drev?: number | null
          ean_fee_mkt?: number | null
          ean_shipping_cost?: number | null
          eprice_eu_preparation_days?: number
          eprice_fee_drev?: number | null
          eprice_fee_mkt?: number | null
          eprice_include_eu?: boolean
          eprice_it_preparation_days?: number
          eprice_preparation_days?: number
          eprice_shipping_cost?: number | null
          fee_drev?: number
          fee_mkt?: number
          id?: string
          mediaworld_eu_preparation_days?: number
          mediaworld_fee_drev?: number | null
          mediaworld_fee_mkt?: number | null
          mediaworld_include_eu?: boolean
          mediaworld_it_preparation_days?: number
          mediaworld_preparation_days?: number
          mediaworld_shipping_cost?: number | null
          shipping_cost?: number
          updated_at?: string | null
        }
        Relationships: []
      }
      sync_config: {
        Row: {
          daily_time: string | null
          enabled: boolean
          frequency_minutes: number
          id: number
          last_disabled_reason: string | null
          max_attempts: number
          max_retries: number
          notification_mode: string
          notify_on_warning: boolean
          retry_delay_minutes: number
          run_timeout_minutes: number
          schedule_type: string
          updated_at: string
        }
        Insert: {
          daily_time?: string | null
          enabled?: boolean
          frequency_minutes?: number
          id?: number
          last_disabled_reason?: string | null
          max_attempts?: number
          max_retries?: number
          notification_mode?: string
          notify_on_warning?: boolean
          retry_delay_minutes?: number
          run_timeout_minutes?: number
          schedule_type?: string
          updated_at?: string
        }
        Update: {
          daily_time?: string | null
          enabled?: boolean
          frequency_minutes?: number
          id?: number
          last_disabled_reason?: string | null
          max_attempts?: number
          max_retries?: number
          notification_mode?: string
          notify_on_warning?: boolean
          retry_delay_minutes?: number
          run_timeout_minutes?: number
          schedule_type?: string
          updated_at?: string
        }
        Relationships: []
      }
      sync_events: {
        Row: {
          created_at: string
          details: Json | null
          id: string
          level: string
          message: string
          run_id: string
          step: string | null
        }
        Insert: {
          created_at?: string
          details?: Json | null
          id?: string
          level: string
          message: string
          run_id: string
          step?: string | null
        }
        Update: {
          created_at?: string
          details?: Json | null
          id?: string
          level?: string
          message?: string
          run_id?: string
          step?: string | null
        }
        Relationships: []
      }
      sync_locks: {
        Row: {
          acquired_at: string
          invocation_id: string | null
          lease_until: string
          lock_name: string
          run_id: string
          updated_at: string
        }
        Insert: {
          acquired_at?: string
          invocation_id?: string | null
          lease_until: string
          lock_name: string
          run_id: string
          updated_at?: string
        }
        Update: {
          acquired_at?: string
          invocation_id?: string | null
          lease_until?: string
          lock_name?: string
          run_id?: string
          updated_at?: string
        }
        Relationships: []
      }
      sync_runs: {
        Row: {
          attempt: number
          cancel_requested: boolean
          cancelled_by_user: boolean
          created_at: string
          error_details: Json | null
          error_message: string | null
          file_manifest: Json
          finished_at: string | null
          id: string
          location_warnings: Json
          metrics: Json
          runtime_ms: number | null
          started_at: string
          status: string
          steps: Json
          trigger_type: string
          warning_count: number
        }
        Insert: {
          attempt?: number
          cancel_requested?: boolean
          cancelled_by_user?: boolean
          created_at?: string
          error_details?: Json | null
          error_message?: string | null
          file_manifest?: Json
          finished_at?: string | null
          id?: string
          location_warnings?: Json
          metrics?: Json
          runtime_ms?: number | null
          started_at?: string
          status?: string
          steps?: Json
          trigger_type: string
          warning_count?: number
        }
        Update: {
          attempt?: number
          cancel_requested?: boolean
          cancelled_by_user?: boolean
          created_at?: string
          error_details?: Json | null
          error_message?: string | null
          file_manifest?: Json
          finished_at?: string | null
          id?: string
          location_warnings?: Json
          metrics?: Json
          runtime_ms?: number | null
          started_at?: string
          status?: string
          steps?: Json
          trigger_type?: string
          warning_count?: number
        }
        Relationships: []
      }
    }
    Views: {
      [_ in never]: never
    }
    Functions: {
      add_self_as_admin: { Args: never; Returns: boolean }
      invoke_cron_tick: { Args: never; Returns: undefined }
      jsonb_deep_merge: { Args: { base: Json; patch: Json }; Returns: Json }
      log_sync_event: {
        Args: {
          p_details?: Json
          p_level: string
          p_message: string
          p_run_id: string
        }
        Returns: {
          event_id: string
          new_warning_count: number
        }[]
      }
      merge_sync_run_metrics: {
        Args: { p_patch: Json; p_run_id: string }
        Returns: undefined
      }
      merge_sync_run_step: {
        Args: { p_patch: Json; p_run_id: string; p_step_name: string }
        Returns: undefined
      }
      release_sync_lock: {
        Args: { p_lock_name: string; p_run_id: string }
        Returns: boolean
      }
      set_step_in_progress: {
        Args: { p_extra?: Json; p_run_id: string; p_step_name: string }
        Returns: undefined
      }
      try_acquire_sync_lock:
        | {
            Args: {
              p_lock_name: string
              p_run_id: string
              p_ttl_seconds: number
            }
            Returns: boolean
          }
        | {
            Args: {
              p_invocation_id?: string
              p_lock_name: string
              p_run_id: string
              p_ttl_seconds: number
            }
            Returns: boolean
          }
    }
    Enums: {
      [_ in never]: never
    }
    CompositeTypes: {
      [_ in never]: never
    }
  }
}

type DatabaseWithoutInternals = Omit<Database, "__InternalSupabase">

type DefaultSchema = DatabaseWithoutInternals[Extract<keyof Database, "public">]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : DefaultSchemaTableNameOrOptions extends keyof (DefaultSchema["Tables"] &
        DefaultSchema["Views"])
    ? (DefaultSchema["Tables"] &
        DefaultSchema["Views"])[DefaultSchemaTableNameOrOptions] extends {
        Row: infer R
      }
      ? R
      : never
    : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Insert: infer I
      }
      ? I
      : never
    : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Update: infer U
      }
      ? U
      : never
    : never

export type Enums<
  DefaultSchemaEnumNameOrOptions extends
    | keyof DefaultSchema["Enums"]
    | { schema: keyof DatabaseWithoutInternals },
  EnumName extends DefaultSchemaEnumNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never = never,
> = DefaultSchemaEnumNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
    ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
    : never

export type CompositeTypes<
  PublicCompositeTypeNameOrOptions extends
    | keyof DefaultSchema["CompositeTypes"]
    | { schema: keyof DatabaseWithoutInternals },
  CompositeTypeName extends PublicCompositeTypeNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"]
    : never = never,
> = PublicCompositeTypeNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"][CompositeTypeName]
  : PublicCompositeTypeNameOrOptions extends keyof DefaultSchema["CompositeTypes"]
    ? DefaultSchema["CompositeTypes"][PublicCompositeTypeNameOrOptions]
    : never

export const Constants = {
  public: {
    Enums: {},
  },
} as const
