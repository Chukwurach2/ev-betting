export default function handler(req,res){
  res.setHeader('Cache-Control','no-store');
  if(req.method!=='GET')return res.status(405).json({error:'Method not allowed'});
  return res.status(200).json({version:'2.5.0',status:'limited',
    capabilities:{schedule:true,device_journal:true,cloud_sync:'requires_signed_in_verification',
      odds_key_configured:Boolean(process.env.THE_ODDS_API_KEY),
      scheduler_secret_configured:Boolean(process.env.CRON_SECRET),
      automatic_picks:false},
    blockers:['Market-level validated model and running prediction publisher are required'],
    checked_at:new Date().toISOString()});
}
