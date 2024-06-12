from conftest import run_test, get_tools_jar_path, get_spark_rapids_cli


def test_missing_event_logs(virtualenv, temp_output_dir):
    incorrect_event_logs_path = '/tmp/incorrect_event_logs'
    cmd = [
        get_spark_rapids_cli(),
        'qualification',
        '--eventlogs', incorrect_event_logs_path,
        '-o', temp_output_dir,
        '--tools_jar', get_tools_jar_path(),
        '--verbose'
    ]
    expected_error = [
        'No event logs to process after checking paths, exiting!'
    ]
    run_test(cmd, expected_error=expected_error)
