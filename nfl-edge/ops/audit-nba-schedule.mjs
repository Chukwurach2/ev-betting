import {readFile} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';

const toleranceMs = 15 * 60_000;
const awareTime = value =>
 typeof value === 'string' && /(?:Z|[+-]\d{2}:\d{2})$/.test(value) ? Date.parse(value) : NaN;

function validGame(game){
 return game && typeof game.home_team === 'string' && game.home_team.trim() &&
  typeof game.away_team === 'string' && game.away_team.trim() &&
  game.home_team !== game.away_team && Number.isFinite(awareTime(game.kickoff));
}

export function auditNbaSchedule(official, feed){
 if(!Array.isArray(official) || !official.every(validGame) ||
    feed?.sport !== 'nba' || !Array.isArray(feed.games) || !feed.games.every(validGame) ||
    !Number.isFinite(awareTime(feed.generated_at)))throw Error('Expected reviewed schedule and NBA phone snapshot');

 const asOf=awareTime(feed.generated_at);
 const counts={matched:0,shifted:0,missing:0,ambiguous:0};
 const games=official.filter(game=>awareTime(game.kickoff)>asOf).map(game=>{
  const officialTip=awareTime(game.kickoff);
  const matches=feed.games.filter(row=>row.home_team===game.home_team &&
    row.away_team===game.away_team && Math.abs(awareTime(row.kickoff)-officialTip)<=toleranceMs);
  let status='missing';
  if(matches.length>1)status='ambiguous';
  if(matches.length===1)status=awareTime(matches[0].kickoff)===officialTip?'matched':'shifted';
  counts[status]++;
  return {home_team:game.home_team,away_team:game.away_team,official_tip:game.kickoff,
   status,provider_tip:matches.length===1?matches[0].kickoff:null};
 });
 return {checked_at:feed.generated_at,counts,games};
}

if(process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href){
 const [officialFile,feedFile]=process.argv.slice(2);
 if(!officialFile||!feedFile)throw Error('Usage: node ops/audit-nba-schedule.mjs OFFICIAL_JSON NBA_PHONE_JSON');
 const load=async file=>JSON.parse(await readFile(file,'utf8'));
 console.log(JSON.stringify(auditNbaSchedule(await load(officialFile),await load(feedFile)),null,2));
}
