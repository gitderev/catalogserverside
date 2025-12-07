import { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { Session, User } from '@supabase/supabase-js';

interface AuthContextType {
  isAuthenticated: boolean;
  isLoading: boolean;
  session: Session | null;
  user: User | null;
  login: (password: string) => Promise<{ success: boolean; error?: string }>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [session, setSession] = useState<Session | null>(null);
  const [user, setUser] = useState<User | null>(null);

  // Set up auth state listener and check for existing session
  useEffect(() => {
    // Set up auth state listener FIRST
    const { data: { subscription } } = supabase.auth.onAuthStateChange(
      (event, currentSession) => {
        console.log('Auth state changed:', event, currentSession?.user?.id);
        setSession(currentSession);
        setUser(currentSession?.user ?? null);
        setIsAuthenticated(!!currentSession);
        
        // Only set loading to false after we've processed the auth state
        if (event === 'INITIAL_SESSION') {
          setIsLoading(false);
        }
      }
    );

    // THEN check for existing session
    supabase.auth.getSession().then(({ data: { session: existingSession } }) => {
      console.log('Existing session check:', existingSession?.user?.id);
      setSession(existingSession);
      setUser(existingSession?.user ?? null);
      setIsAuthenticated(!!existingSession);
      setIsLoading(false);
    });

    return () => subscription.unsubscribe();
  }, []);

  const login = async (password: string): Promise<{ success: boolean; error?: string }> => {
    try {
      // Call the edge function to validate password and get Supabase tokens
      const { data, error } = await supabase.functions.invoke('auth-login', {
        body: { password }
      });

      if (error) {
        console.error('Login function error:', error);
        return { success: false, error: 'Errore di connessione' };
      }

      if (!data?.success) {
        return { success: false, error: data?.error || 'Password errata, riprova' };
      }

      // Set the Supabase session with the returned tokens
      if (data.access_token && data.refresh_token) {
        const { data: sessionData, error: sessionError } = await supabase.auth.setSession({
          access_token: data.access_token,
          refresh_token: data.refresh_token,
        });

        if (sessionError) {
          console.error('Failed to set session:', sessionError);
          return { success: false, error: 'Errore durante l\'autenticazione' };
        }

        console.log('Session set successfully:', sessionData.session?.user?.id);
        return { success: true };
      }

      return { success: false, error: 'Risposta del server non valida' };
    } catch (err) {
      console.error('Login error:', err);
      return { success: false, error: 'Errore di connessione' };
    }
  };

  const logout = async () => {
    try {
      await supabase.auth.signOut();
    } catch (err) {
      console.error('Logout error:', err);
    }
    // State will be updated by onAuthStateChange listener
  };

  return (
    <AuthContext.Provider value={{ isAuthenticated, isLoading, session, user, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
