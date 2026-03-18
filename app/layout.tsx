import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "WARNSignal | Quantitative Research Dashboard",
  description: "Event-driven short signal backtest using WARN Act layoff filings",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="min-h-screen antialiased">
        <nav className="border-b border-border bg-card/50 backdrop-blur sticky top-0 z-50">
          <div className="max-w-7xl mx-auto px-4 h-12 flex items-center justify-between">
            <div className="flex items-center gap-6">
              <a href="/" className="text-primary font-bold text-sm tracking-wider">
                WARN<span className="text-foreground">SIGNAL</span>
              </a>
              <div className="flex gap-4 text-xs">
                <a href="/" className="text-muted-foreground hover:text-foreground transition-colors">
                  DASHBOARD
                </a>
                <a href="/filings" className="text-muted-foreground hover:text-foreground transition-colors">
                  FILINGS
                </a>
                <a href="/signals" className="text-muted-foreground hover:text-foreground transition-colors">
                  SIGNALS
                </a>
                <a href="/backtest" className="text-muted-foreground hover:text-foreground transition-colors">
                  BACKTEST
                </a>
              </div>
            </div>
            <div className="text-xs text-muted-foreground">
              v0.1.0
            </div>
          </div>
        </nav>
        <main className="max-w-7xl mx-auto px-4 py-6">
          {children}
        </main>
      </body>
    </html>
  );
}
