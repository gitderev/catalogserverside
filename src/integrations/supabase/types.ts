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
    PostgrestVersion: "13.0.5"
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
          max_retries: number
          retry_delay_minutes: number
          updated_at: string
        }
        Insert: {
          daily_time?: string | null
          enabled?: boolean
          frequency_minutes?: number
          id?: number
          max_retries?: number
          retry_delay_minutes?: number
          updated_at?: string
        }
        Update: {
          daily_time?: string | null
          enabled?: boolean
          frequency_minutes?: number
          id?: number
          max_retries?: number
          retry_delay_minutes?: number
          updated_at?: string
        }
        Relationships: []
      }
      sync_locks: {
        Row: {
          expires_at: string
          lock_key: string
          locked_at: string
          locked_by: string
          run_id: string | null
        }
        Insert: {
          expires_at: string
          lock_key: string
          locked_at?: string
          locked_by: string
          run_id?: string | null
        }
        Update: {
          expires_at?: string
          lock_key?: string
          locked_at?: string
          locked_by?: string
          run_id?: string | null
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
          finished_at: string | null
          id: string
          location_warnings: Json
          metrics: Json
          runtime_ms: number | null
          started_at: string
          status: string
          steps: Json
          trigger_type: string
        }
        Insert: {
          attempt?: number
          cancel_requested?: boolean
          cancelled_by_user?: boolean
          created_at?: string
          error_details?: Json | null
          error_message?: string | null
          finished_at?: string | null
          id?: string
          location_warnings?: Json
          metrics?: Json
          runtime_ms?: number | null
          started_at?: string
          status?: string
          steps?: Json
          trigger_type: string
        }
        Update: {
          attempt?: number
          cancel_requested?: boolean
          cancelled_by_user?: boolean
          created_at?: string
          error_details?: Json | null
          error_message?: string | null
          finished_at?: string | null
          id?: string
          location_warnings?: Json
          metrics?: Json
          runtime_ms?: number | null
          started_at?: string
          status?: string
          steps?: Json
          trigger_type?: string
        }
        Relationships: []
      }
    }
    Views: {
      [_ in never]: never
    }
    Functions: {
      add_self_as_admin: { Args: never; Returns: boolean }
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
