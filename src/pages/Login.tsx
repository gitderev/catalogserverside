import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '@/hooks/useAuth';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Lock, Eye, EyeOff, Settings } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import { supabase } from '@/integrations/supabase/client';

export default function Login() {
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isSettingUp, setIsSettingUp] = useState(false);
  const { login, isAuthenticated } = useAuth();
  const navigate = useNavigate();
  const { toast } = useToast();

  // Redirect if already authenticated
  if (isAuthenticated) {
    navigate('/', { replace: true });
    return null;
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!password.trim()) {
      toast({
        title: 'Errore',
        description: 'Inserisci la password',
        variant: 'destructive',
      });
      return;
    }

    setIsSubmitting(true);
    
    const result = await login(password);
    
    if (result.success) {
      navigate('/', { replace: true });
    } else {
      toast({
        title: 'Accesso negato',
        description: result.error || 'Password errata, riprova',
        variant: 'destructive',
      });
      setPassword('');
    }
    
    setIsSubmitting(false);
  };

  const handleSetupAdmin = async () => {
    if (!password.trim()) {
      toast({
        title: 'Errore',
        description: 'Inserisci prima la password di accesso per il setup',
        variant: 'destructive',
      });
      return;
    }

    setIsSettingUp(true);
    
    try {
      const { data, error } = await supabase.functions.invoke('setup-admin-user', {
        body: { setupSecret: password }
      });

      if (error) {
        throw error;
      }

      if (data.success) {
        toast({
          title: 'Setup completato',
          description: data.message,
        });
      } else {
        toast({
          title: 'Errore setup',
          description: data.error || 'Errore sconosciuto',
          variant: 'destructive',
        });
      }
    } catch (error: any) {
      toast({
        title: 'Errore setup',
        description: error.message || 'Errore durante il setup',
        variant: 'destructive',
      });
    }
    
    setIsSettingUp(false);
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-background to-muted p-4">
      <Card className="w-full max-w-md shadow-xl">
        <CardHeader className="text-center space-y-4">
          <div className="mx-auto w-16 h-16 bg-primary/10 rounded-full flex items-center justify-center">
            <Lock className="w-8 h-8 text-primary" />
          </div>
          <CardTitle className="text-2xl">Accesso Richiesto</CardTitle>
          <CardDescription>
            Inserisci la password per accedere all'applicazione
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-6">
            <div className="relative">
              <Input
                type={showPassword ? 'text' : 'password'}
                placeholder="Password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="pr-10"
                autoFocus
                disabled={isSubmitting || isSettingUp}
              />
              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors"
                tabIndex={-1}
              >
                {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
              </button>
            </div>
            <Button 
              type="submit" 
              className="w-full" 
              disabled={isSubmitting || isSettingUp}
            >
              {isSubmitting ? 'Verifica in corso...' : 'Accedi'}
            </Button>
          </form>
          
          {/* Setup button - remove after first use */}
          <div className="mt-6 pt-6 border-t border-border">
            <p className="text-xs text-muted-foreground mb-3 text-center">
              Prima configurazione? Inserisci la password e clicca Setup.
            </p>
            <Button 
              variant="outline" 
              className="w-full"
              onClick={handleSetupAdmin}
              disabled={isSubmitting || isSettingUp}
            >
              <Settings className="w-4 h-4 mr-2" />
              {isSettingUp ? 'Setup in corso...' : 'Setup Admin User'}
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
