-- Publisher writes only through a privileged backend. The phone can read its owner rows.
CREATE TABLE IF NOT EXISTS public.research_leans (
 id text PRIMARY KEY,
 user_id text NOT NULL,
 payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
 published_at timestamptz NOT NULL DEFAULT now(),
 CHECK (payload->>'rights'='private_original_analysis'),
 CHECK (payload->>'id'=id)
);
ALTER TABLE public.research_leans ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.research_leans FORCE ROW LEVEL SECURITY;
REVOKE ALL ON public.research_leans FROM PUBLIC, anonymous, authenticated;
GRANT SELECT ON public.research_leans TO authenticated;
CREATE POLICY research_leans_owner_read ON public.research_leans
 FOR SELECT TO authenticated USING (user_id=auth.user_id());
CREATE INDEX research_leans_owner_time ON public.research_leans(user_id,published_at DESC);
