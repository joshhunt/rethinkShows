request = require 'request-promise'
r       = require('rethinkdbdash')()
_       = require 'underscore'

shows    = {}
rawShows = {}

Shows = r.table('shows')
Episodes = r.table('episodes')

Shows.delete()
    .then ->
        Episodes.delete()
    .then ->
        request.get('http://tv-api-cat.api.9jumpin.com.au/episodes?fields=true&take=10000')
    .then (data) ->
        episodes = JSON.parse(data).payload

        episodesByShowSlug = _.chain episodes
            .groupBy (episode) -> episode.show.slug
            .each (episodesForShow, showSlug) ->
                {show} = episodesForShow[0]
                show.episodes = episodesForShow

                rawShows[showSlug] = show

        rawShows = _.map rawShows, (rawShow) -> rawShow

        rawShows
    .then (rawShows) ->
        shows = _.map rawShows, (show) ->
            {
                slug: show.slug
                title: show.title
                description: show.description
                drm: show.drm
                genre: show.genre
                channel: show.tvChannel
                image: show.image.showImage
            }

        Shows.insert(shows, {returnChanges: true})
    .then ({changes}) ->
        changes.forEach ({new_val}, index) ->
            rawShows[index].id = new_val.id
        rawShows
    .then (rawShows) ->
        episodesToAdd = []

        rawShows.forEach (show) ->
            show.episodes.forEach (episode) ->
                episodesToAdd.push {
                    season: episode.seasonTitle
                    title: episode.title
                    slug: episode.slug
                    description: episode.description
                    airDate: new Date(episode.airDate)
                    expiryDate: new Date(episode.videoExpiryDate)
                    episodeNumber: episode.episodeNumber
                    duration: episode.durationSeconds
                    image: episode.images.videoStill
                    showId: show.id
                }

        Episodes.insert(episodesToAdd, {returnChanges: true})
    .then (ch) ->
        console.log ch
    .catch (err) ->
        console.error err.stack
    .finally ->
        process.exit()


# r.table('shows')
#   .eqJoin('id', r.table('episodes'), {index: 'showId'})
#   .map(function(row) {
#     return row('left').merge({
#       episodes: row('right')
#     })
#   })

# r.table('shows')
#   .merge(
#     {
#       episodes: r.table('episodes')
#         .getAll(r.row('id'), {index: 'showId'})
#         .coerceTo('array')
#     }
#   )

# r.table('episodes')
#     .filter(r.row('expiryDate').lt(r.now().add(60 * 60 * 24 * 7)))
#     .count()