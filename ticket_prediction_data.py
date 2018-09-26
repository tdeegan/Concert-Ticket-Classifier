import pandas as pd
from sqlalchemy import create_engine
import re

class Ticket_PredictionData(object):

    # Initialize
    def __init__(self):

        # Create engine
        self._engine = create_engine(
            'postgresql+psycopg2://awsuser:Capstone1@redshift-cluster-1.cah6qt0iybst.us-east-2.redshift.amazonaws.com:5439/dev')

        # Test connection
        self._test_conn()

        # Store DataFrames
        self._main = None
        self._perf_raw = None
        self._perf = None
        self.final = None


    # Test connection... confirm that query works
    def _test_conn(self):
        try:
            pd.read_sql('SELECT COUNT(*) FROM stubhub.events_df', self._engine)
            print('Connection working!')
        except:
            print('Connection failed :(')


    # Query to return foundational DataFrame of features
    def _get_main(self):
        print('Querying for main table')

        self._main = \
            pd.read_sql(
                    '''
                    SELECT
                        r.listing_id,
                        r.ticket_splits_option,
                        r.date,
                        r.indicator,
                        d.days_until_show,
                        d.dow_listing_avail,
                        d.dow_show,
                        e.geos,
                        e.event_id,
                        e.category,
                        e.event_parking,
                        e.venue_id,
                        t.dirty_ticket_ind,
                        t.price_curr,
                        t.quantity,
                        t.zone_name,
                        ts.popularity,
                        ts.totallistings,
                        t.price_curr - ts.minprice AS price_over_min,
                        ts.minprice,
                        r.ticket_splits_option = t.quantity AS full_listing
                    FROM
                        sandbox.response_var r
                    JOIN
                      stubhub.tickets_df t
                      ON
                        t.listing_id = r.listing_id
                        AND t.date_accessed = r.date_accessed
                    JOIN
                        stubhub.events_ticket_summary ts
                        ON
                            ts.event_id = t.event_id
                            AND ts.date_accessed = t.date_accessed
                    JOIN
                        sandbox.features_date d
                        ON
                            d.event_id = t.event_id
                            AND d.date_accessed = r.date_accessed
                    JOIN
                        stubhub.events_df e
                        ON
                            e.event_id = t.event_id
                    WHERE e.geos = 'Boston' 
                        ''',
                    self._engine)

        print('Got main table')


    def _simp_venues(self, max_venues=None, min_events = 5):
        # Get main DataFrame if we don't have it yet
        if self._main is None:
            self._get_main()

        print('Simplifying venues')

        # Get a list of venues to keep that have more than the min events threshold
        events_per_ven = self._main.groupby('venue_id').event_id.nunique()
        if max_venues:
            events_per_ven = events_per_ven.nlargest(max_venues)
        vens_keep = events_per_ven[events_per_ven > min_events].index.tolist()

        # Make venue id a string so it can be dummied;
        ## use '0' if the venue is below the threshold
        self._venue_id_simp = self._main.venue_id.apply(
                                    lambda x: str(x) if x in vens_keep else '0')

        print('Venues simplified')


    def _simp_events(self, max_events = None, min_listings = 1000):
        if self._main is None:
            self._get_main()

        print('Simplifying events')

        # Get a list of events to keep that have more than the min listings threshold
        tix_per_event = self._main.groupby('event_id').event_id.count()
        if max_events:
            tix_per_event = tix_per_event.nlargest(max_events)
        events_keep = tix_per_event[tix_per_event > min_listings].index.tolist()

        # Make event id a string so it can be dummied;
        ## use '0' if the event is below the threshold
        self._event_id_simp = self._main.event_id.apply(
                                    lambda x: str(x) if x in events_keep else '0')

        print('Events simplified')


    def _simp_zones(self, max_zones = None, min_listings = 1000):
        if self._main is None:
            self._get_main()

        print('Simplifying zones')

        # Get a list of events to keep that have more than the min listings threshold
        tix_per_zone = self._main.groupby('zone_name').zone_name.count()
        if max_zones:
            tix_per_zone = tix_per_zone.nlargest(max_zones)
        zones_keep = tix_per_zone[tix_per_zone > min_listings].index.tolist()

        # Make event id a string so it can be dummied;
        ## use '0' if the event is below the threshold
        self._zone_name_simp = self._main.zone_name.apply(
                                    lambda x: '0' if x == '' else str(x) if x in zones_keep else '0')

        print('Zones simplified')


    def _get_perf(self, max_perf = None, min_events = 10):

        # Get performers DataFrame if we have not yet
        if self._perf_raw is None:
            print('Querying for performers')

            self._perf_raw = pd.read_sql(
                '''SELECT * FROM
                    (SELECT
                      p.performer_id,
                      p.event_id,
                      COUNT(performer_id) OVER (PARTITION BY performer_id) AS perf_events,
                      COUNT(event_id) OVER (PARTITION BY event_id) AS event_perf_count
                    FROM
                      stubhub.events_perf p)
                ''', self._engine)

        print('Dummying performers')

        perf = self._perf_raw

        if max_perf:
            perf_keep = perf.groupby('performer_id').event_id.nunique().nlargest(max_perf).index.tolist()
            perf['performer_id'] = perf.performer_id.apply(lambda x: x if x in perf_keep else 0)

        perf['performer_id'] = perf.apply(
                        lambda x: '0' if x['perf_events'] < min_events else str(x['performer_id']),
                        axis=1)

        perf = pd.get_dummies(perf[['event_id', 'event_perf_count', 'performer_id']], drop_first=True)

        self._perf = perf.groupby(['event_id', 'event_perf_count']).max().reset_index()

        print('Performers dummied')


    def get_data(self, venues=True, max_venues=None, venues_min_events=5,
                        events=True, max_events=None, events_min_listings=1000,
                        zones=True, max_zones=None, zones_min_listings=1000,
                        performers=True, max_perf=None, perf_min_events=10,
                        keep_dow=False):

        # Start with main dataframe
        if self._main is None:
            self._get_main()
        else:
            print("Already have data... don't need to query again")
        df = self._main.copy()

        # Simplify venues if user selected venues
        if venues:
            self._simp_venues(max_venues = max_venues, min_events = venues_min_events)
            df['venue_id_simp'] = self._venue_id_simp

        # Simplify events if user selected venues
        if events:
            self._simp_events(min_listings = events_min_listings, max_events = max_events)
            df['event_id_simp'] = self._event_id_simp


        # Simply zones if user selected zones
        if zones:
            self._simp_zones(min_listings = zones_min_listings, max_zones = max_zones)
            df['zone_name_simp'] = self._zone_name_simp

        # Merge in performers if user selected performers
        if performers:
            self._get_perf(min_events = perf_min_events, max_perf = max_perf)

            print('Concatenating dummied performers')

            # Use a dataframe with just the event IDs to expand the performer dummies to
            ## match the full DataFrame; fill NA's with 0s
            df_perf = df[['event_id']].merge(self._perf, how='left', on='event_id')
            df_perf = df_perf.fillna(0)

            # Concatanate the full performers dummy DF with the main DF
            df = pd.concat([df, df_perf], axis = 1)

            print('Performers concatanated')

        print('Starting final cleanup')

        # For numbers that should be dummied, turn into strings
        df[['dow_listing_avail', 'dow_show']] = df[['dow_listing_avail', 'dow_show']].applymap(str)

        # Create index for holdout
        holdout = df.date == df.date.max()

        # Drop IDs and dates (should not be features)
        df = df.drop(['event_id', 'venue_id', 'date', 'zone_name'], axis=1)

        # Strip whitespace off of geos and categories
        df[['geos', 'category']] = df[['geos', 'category']].applymap(
                                        lambda x: x.strip().replace(' ', '_').replace('/', ''))

        # Impute dirty ticket indicator to be False
        df['dirty_ticket_ind'] = df.dirty_ticket_ind.apply(lambda x: 'False' if x == '' else x)

        # At this point we should not use the dow for the listing in model training
        ##since our holdout set will be a dow we do not have in the training set yet
        if not keep_dow:
            df = df.drop('dow_listing_avail', axis=1)

        df = pd.get_dummies(df, drop_first=True)

        self.final = df

        X = self.final.drop(['listing_id', 'indicator'], axis=1)
        Y = self.final['indicator']

        print('Done')

        return X[~holdout], X[holdout], Y[~holdout], Y[holdout]
