import {realpathSync} from 'node:fs';
import {readFile} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';

const toleranceMs = 15 * 60_000;
const easternDay = new Intl.DateTimeFormat('en-US', {
 timeZone:'America/New_York', year:'numeric', month:'2-digit', day:'2-digit'
});
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
 const games=[], listedPastOfficialTip=[], sameDayStartConflicts=[];
 for(const game of official){
  const officialTip=awareTime(game.kickoff);
  const sameMatchup=feed.games.filter(row=>row.home_team===game.home_team &&
    row.away_team===game.away_team);
  const matches=sameMatchup.filter(row=>Math.abs(awareTime(row.kickoff)-officialTip)<=toleranceMs);
  if(!matches.length){
   const officialDay=easternDay.format(officialTip);
   const candidates=sameMatchup.filter(row=>easternDay.format(awareTime(row.kickoff))===officialDay);
   if(candidates.length){
    sameDayStartConflicts.push({
     home_team:game.home_team, away_team:game.away_team, official_tip:game.kickoff,
     provider_listings:candidates.map(row=>({
      provider_tip:row.kickoff, delta_minutes:(awareTime(row.kickoff)-officialTip)/60_000
     }))
    });
   }
  }
  if(officialTip<=asOf){
   for(const row of matches){
    if(awareTime(row.kickoff)>asOf){
     listedPastOfficialTip.push({home_team:game.home_team,away_team:game.away_team,
      official_tip:game.kickoff,provider_tip:row.kickoff,
      listed_entries:Array.isArray(row.picks)?row.picks.length:null});
    }
   }
   continue;
  }
  let status='missing';
  if(matches.length>1)status='ambiguous';
  if(matches.length===1)status=awareTime(matches[0].kickoff)===officialTip?'matched':'shifted';
  counts[status]++;
  games.push({home_team:game.home_team,away_team:game.away_team,official_tip:game.kickoff,
   status,provider_tip:matches.length===1?matches[0].kickoff:null});
 }
 return {checked_at:feed.generated_at,counts,games,
  listed_past_official_tip:listedPastOfficialTip,
  same_day_start_conflicts:sameDayStartConflicts};
}

if(process.argv[1] && import.meta.url===pathToFileURL(realpathSync(process.argv[1])).href){
 const [officialFile,feedFile]=process.argv.slice(2);
 if(!officialFile||!feedFile)throw Error('Usage: node ops/audit-nba-schedule.mjs OFFICIAL_JSON NBA_PHONE_JSON');
 const load=async file=>JSON.parse(await readFile(file,'utf8'));
 console.log(JSON.stringify(auditNbaSchedule(await load(officialFile),await load(feedFile)),null,2));
}
