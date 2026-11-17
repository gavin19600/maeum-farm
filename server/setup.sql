create extension if not exists pgcrypto with schema extensions;
create table if not exists public.docs(
  col text not null, id text not null, data jsonb not null,
  zone text generated always as (data->>'zone') stored,
  updated_at timestamptz not null default now(),
  primary key(col,id));
create index if not exists docs_col_zone on public.docs(col,zone);
create index if not exists docs_col_upd on public.docs(col,updated_at);
alter table public.docs enable row level security;
drop policy if exists docs_read on public.docs;
create policy docs_read on public.docs for select to anon using (col in ('lots','cheers','pebbles','tree'));
create table if not exists public.priv(id text not null, k text not null, data jsonb not null, updated_at timestamptz not null default now(), primary key(id,k));
alter table public.priv enable row level security;
create unlogged table if not exists public.pres(id text primary key, w text not null, p jsonb not null, t timestamptz not null default now());
create index if not exists pres_w_t on public.pres(w,t);
alter table public.pres enable row level security;
create or replace function public.mf_uid(secret text) returns text language sql immutable set search_path=public,extensions as $$
  select 'u_'||substr(encode(digest(secret,'sha256'),'hex'),1,24) $$;
create or replace function public.mf_put(secret text, c text, d jsonb) returns timestamptz language plpgsql security definer set search_path=public,extensions as $$
declare me text; ts timestamptz := now();
begin
  if secret is null or length(secret)<32 then raise exception 'bad secret'; end if;
  if c not in ('lots','cheers','pebbles','tree') then raise exception 'bad collection'; end if;
  if octet_length(d::text)>8000 then raise exception 'too large'; end if;
  me := mf_uid(secret);
  insert into docs(col,id,data,updated_at) values(c,me,d,ts)
  on conflict(col,id) do update set data=excluded.data, updated_at=excluded.updated_at;
  return ts;
end $$;
create or replace function public.mf_del(secret text, c text) returns void language plpgsql security definer set search_path=public,extensions as $$
begin
  if secret is null or length(secret)<32 then raise exception 'bad secret'; end if;
  delete from docs where col=c and id=mf_uid(secret);
end $$;
create or replace function public.mf_priv_put(secret text, key text, d jsonb) returns void language plpgsql security definer set search_path=public,extensions as $$
begin
  if secret is null or length(secret)<32 then raise exception 'bad secret'; end if;
  if key not in ('farm') then raise exception 'bad key'; end if;
  if octet_length(d::text)>260000 then raise exception 'too large'; end if;
  insert into priv(id,k,data,updated_at) values(mf_uid(secret),key,d,now())
  on conflict(id,k) do update set data=excluded.data, updated_at=now();
end $$;
create or replace function public.mf_priv_get(secret text, key text) returns jsonb language sql security definer set search_path=public,extensions as $$
  select data from priv where id=mf_uid(secret) and k=key $$;
create or replace function public.mf_beat(secret text, world text, pp jsonb, since double precision) returns jsonb language plpgsql security definer set search_path=public,extensions as $$
declare me text; res jsonb; cnt int;
begin
  if secret is null or length(secret)<32 then raise exception 'bad secret'; end if;
  if world is null or length(world)>12 then raise exception 'bad world'; end if;
  me := mf_uid(secret);
  if pp is not null then
    if octet_length(pp::text)>600 then raise exception 'too large'; end if;
    insert into pres(id,w,p,t) values(me,world,pp,now())
    on conflict(id) do update set w=excluded.w,p=excluded.p,t=now();
  else
    update pres set t=now(), w=world where id=me;
  end if;
  if random()<0.002 then delete from pres where t<now()-interval '1 hour'; end if;
  select count(*) into cnt from pres where t>now()-interval '25 seconds';
  select coalesce(jsonb_agg(jsonb_build_object('i',q.id,'p',q.p,'t',extract(epoch from q.t))),'[]'::jsonb) into res
  from (select id,p,t from pres where w=world and id<>me and t>now()-interval '25 seconds' and extract(epoch from t)>coalesce(since,0) order by t desc limit 150) q;
  return jsonb_build_object('now',extract(epoch from now()),'n',cnt,'peers',res);
end $$;
revoke all on function public.mf_put(text,text,jsonb), public.mf_del(text,text), public.mf_priv_put(text,text,jsonb), public.mf_priv_get(text,text), public.mf_beat(text,text,jsonb,double precision) from public;
grant execute on function public.mf_put(text,text,jsonb), public.mf_del(text,text), public.mf_priv_put(text,text,jsonb), public.mf_priv_get(text,text), public.mf_beat(text,text,jsonb,double precision), public.mf_uid(text) to anon;
grant select on public.docs to anon;
