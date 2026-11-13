// Called inside the publisher transaction.
export async function insertPrivateLean(db, owner, payload) {
 const params=[payload.id,owner,JSON.stringify(payload)];
 const inserted=await db.query(
  'INSERT INTO public.research_leans(id,user_id,payload) VALUES ($1,$2,$3) ON CONFLICT(id) DO NOTHING',
  params,
 );
 if(inserted.rowCount===1)return;
 const existing=await db.query(
  'SELECT 1 FROM public.research_leans WHERE id=$1 AND user_id=$2 AND payload=$3::jsonb',
  params,
 );
 if(existing.rowCount!==1)throw Error('Private research ID already in use');
}
