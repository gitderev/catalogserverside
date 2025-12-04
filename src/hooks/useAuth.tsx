import { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { supabase } from '@/integrations/supabase/client';

interface AuthContextType {
  isAuthenticated: boolean;
  isLoading: boolean;
  login: (password: string) => Promise<{ success: boolean; error?: string }>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | null>(null);

const AUTH_TOKEN_KEY = 'app_auth_token';

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoading, setIsLoading] = useState(true);

  // Validate token on mount
  useEffect(() => {
    const validateSession = async () => {
      const token = localStorage.getItem(AUTH_TOKEN_KEY);
      
      if (!token) {
        setIsLoading(false);
        return;
      }

      try {
        const { data, error } = await supabase.functions.invoke('auth-validate', {
          body: { token }
        });

        if (error) {
          console.error('Session validation error:', error);
          localStorage.removeItem(AUTH_TOKEN_KEY);
          setIsAuthenticated(false);
        } else if (data?.valid) {
          setIsAuthenticated(true);
        } else {
          localStorage.removeItem(AUTH_TOKEN_KEY);
          setIsAuthenticated(false);
        }
      } catch (err) {
        console.error('Session validation failed:', err);
        localStorage.removeItem(AUTH_TOKEN_KEY);
        setIsAuthenticated(false);
      }
      
      setIsLoading(false);
    };

    validateSession();
  }, []);

  const login = async (password: string): Promise<{ success: boolean; error?: string }> => {
    try {
      const { data, error } = await supabase.functions.invoke('auth-login', {
        body: { password }
      });

      if (error) {
        return { success: false, error: 'Errore di connessione' };
      }

      if (data?.success && data?.token) {
        localStorage.setItem(AUTH_TOKEN_KEY, data.token);
        setIsAuthenticated(true);
        return { success: true };
      }

      return { success: false, error: data?.error || 'Password errata, riprova' };
    } catch (err) {
      console.error('Login error:', err);
      return { success: false, error: 'Errore di connessione' };
    }
  };

  const logout = () => {
    localStorage.removeItem(AUTH_TOKEN_KEY);
    setIsAuthenticated(false);
  };

  return (
    <AuthContext.Provider value={{ isAuthenticated, isLoading, login, logout }}>
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
