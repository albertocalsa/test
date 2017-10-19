from source.pipelines.lastfm_etl import LastFmETL
from source.reports.lastfm_aggregates import LastFmAggregates


def main():
    lastfm_file_path = 'data/lastfm-dataset-1K.tar.gz'
    pipeline = LastFmETL()
    data = pipeline.process_lastfm_file(lastfm_file_path, 'data/unzipped')

    reporter = LastFmAggregates(data)

    report_a = reporter.count_distinct_songs_per_user()
    print('Part A')
    report_a.show()

    report_b = reporter.get_top_listened_songs(n=100)
    print('Part B')
    report_b.show()

    report_c = reporter.get_top_user_sessions(n=10, session_length_mins=20)
    print('Part C')
    report_c.show()


main()
