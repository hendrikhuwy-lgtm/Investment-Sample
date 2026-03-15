import { Link, useRoute } from "wouter";
import { RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useQueryClient } from "@tanstack/react-query";

export function NavBar() {
  const [isDailyBrief] = useRoute("/");
  const [isBlueprint] = useRoute("/blueprint");
  const queryClient = useQueryClient();

  const handleRefresh = () => {
    queryClient.invalidateQueries();
  };

  const today = new Date().toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric'
  });

  return (
    <header className="sticky top-0 z-50 w-full bg-background/95 backdrop-blur border-b border-border">
      <div className="flex h-14 items-center px-4 md:px-6 lg:px-8 max-w-[1400px] mx-auto w-full">
        <div className="flex w-1/3 justify-start">
          <span className="font-mono font-bold tracking-wider text-foreground">INVESTMENT OS</span>
        </div>
        
        <div className="flex w-1/3 justify-center space-x-6">
          <Link href="/" className={`px-2 py-1 text-sm font-medium transition-colors hover:text-primary ${isDailyBrief ? "text-primary" : "text-muted-foreground"}`}>
            Daily Brief
          </Link>
          <Link href="/blueprint" className={`px-2 py-1 text-sm font-medium transition-colors hover:text-warning ${isBlueprint ? "text-warning" : "text-muted-foreground"}`}>
            Blueprint
          </Link>
        </div>

        <div className="flex w-1/3 justify-end items-center space-x-4">
          <span className="text-sm text-muted-foreground font-mono">
            {today}
          </span>
          <Button variant="ghost" size="icon" onClick={handleRefresh} className="h-8 w-8 text-muted-foreground hover:text-foreground">
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>
      </div>
      <div className={`h-[2px] w-full transition-colors duration-300 ${isBlueprint ? 'bg-warning' : 'bg-primary'}`} />
    </header>
  );
}
