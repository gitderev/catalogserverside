import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Verify setup secret (optional security layer)
    const { setupSecret } = await req.json();
    const expectedSecret = Deno.env.get("APP_ACCESS_PASSWORD");
    
    if (setupSecret !== expectedSecret) {
      return new Response(
        JSON.stringify({ error: "Unauthorized" }),
        { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const adminEmail = Deno.env.get("ADMIN_SUPABASE_EMAIL")!;
    const adminPassword = Deno.env.get("ADMIN_SUPABASE_PASSWORD")!;

    if (!adminEmail || !adminPassword) {
      return new Response(
        JSON.stringify({ error: "Admin credentials not configured in secrets" }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, {
      auth: {
        autoRefreshToken: false,
        persistSession: false,
      },
    });

    // Check if user already exists
    const { data: existingUsers, error: listError } = await supabaseAdmin.auth.admin.listUsers();
    
    if (listError) {
      console.error("Error listing users:", listError);
      return new Response(
        JSON.stringify({ error: "Failed to check existing users" }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    let userId: string;
    const existingUser = existingUsers.users.find(u => u.email === adminEmail);

    if (existingUser) {
      console.log("Admin user already exists:", existingUser.id);
      userId = existingUser.id;
    } else {
      // Create the admin user
      const { data: newUser, error: createError } = await supabaseAdmin.auth.admin.createUser({
        email: adminEmail,
        password: adminPassword,
        email_confirm: true,
      });

      if (createError) {
        console.error("Error creating user:", createError);
        return new Response(
          JSON.stringify({ error: `Failed to create admin user: ${createError.message}` }),
          { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
        );
      }

      console.log("Admin user created:", newUser.user.id);
      userId = newUser.user.id;
    }

    // Check if user is already in admin_users
    const { data: existingAdmin, error: checkError } = await supabaseAdmin
      .from("admin_users")
      .select("user_id")
      .eq("user_id", userId)
      .maybeSingle();

    if (checkError) {
      console.error("Error checking admin_users:", checkError);
      return new Response(
        JSON.stringify({ error: "Failed to check admin_users table" }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    if (existingAdmin) {
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: "Admin user already configured",
          userId 
        }),
        { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Add user to admin_users table
    const { error: insertError } = await supabaseAdmin
      .from("admin_users")
      .insert({ user_id: userId });

    if (insertError) {
      console.error("Error inserting into admin_users:", insertError);
      return new Response(
        JSON.stringify({ error: `Failed to add user to admin_users: ${insertError.message}` }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    return new Response(
      JSON.stringify({ 
        success: true, 
        message: "Admin user created and added to admin_users",
        userId 
      }),
      { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );

  } catch (error) {
    console.error("Setup error:", error);
    return new Response(
      JSON.stringify({ error: error instanceof Error ? error.message : String(error) }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }
});
