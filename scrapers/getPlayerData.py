### Get recently scraped matches
# Get wordclouds and ward placements/dewards
def getData():
    import requests
    import json
    print('Accessing Player Data...')
    playerLink = 'https://api.opendota.com/api/players/46313030'
    r = requests.get(playerLink)
    payload = r.json()

    # Dump matches into table
    with open('greet.txt', 'a+', encoding='utf8') as f:
        json.dump(payload, f)
    return 'Player data saved to outfile.'

### Check if new players to be added to players table
def respond():
    return 'Greet Responded Again'
