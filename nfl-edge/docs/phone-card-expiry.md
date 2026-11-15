# Phone card expiry

The app removes research entries at their earliest stored expiry or the end of
its 15-minute quote age limit, even while a refresh is pending. It removes
games at kickoff and rechecks loaded data when the app returns to the
foreground. Switching sports or selected books clears the old slate
immediately. The local expiry timer makes no network requests.

A fake-clock regression holds a refresh open across an entry's expiry and
game kickoff. The app removes both at the respective boundaries and sends
no extra request. This check uses a synthetic card and does not verify live
phone rendering or change collection cadence.
