export function buildEngineVerdict({enginePicks, gamesWithQuotes, slateGames}) {
  if (enginePicks > 0) {
    return `${enginePicks} qualifying shadow edge(s) under review`;
  }
  if (slateGames === 0) {
    return 'Pending — no games are available for this slate';
  }
  if (gamesWithQuotes === 0) {
    return 'Pending — no stored quotes are available; zero picks is not evidence of no edge';
  }
  if (gamesWithQuotes < slateGames) {
    return `Pending — stored quote coverage is incomplete (${gamesWithQuotes}/${slateGames} games); zero picks is not evidence of no edge`;
  }
  return 'No qualifying shadow picks are recorded under the current gates';
}
