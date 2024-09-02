import json
from datetime import datetime

# Function to generate the updated analysis report with correct metrics
def generate_comprehensive_analysis_report(details, executors, storage_rdd, stages, jobs):
    report = []

    # 1. Overall Structure and Stage Breakdown
    report.append("Detailed Analysis Implementation in PySpark:\n")
    report.append("1. Overall Structure and Stage Breakdown:")

    total_stages = len(stages)
    executed_stages = sum(1 for stage in stages if stage['status'] == 'COMPLETE')
    skipped_stages = total_stages - executed_stages

    report.append(f"The execution is broken up into {total_stages} stages across multiple jobs.")
    report.append(f"- Total stages created: {total_stages}")
    report.append(f"- Stages actually executed: {executed_stages}")
    report.append(f"- Stages skipped: {skipped_stages}\n")

    # 2. DAG Analysis
    report.append("2. DAG Analysis:\n")

    def print_stage(stage):
        report.append(f"Stage {stage['stageId']}:")
        report.append(f"- Name: {stage['name']}")
        report.append(f"- Status: {stage['status']}")
        report.append(f"- Number of Tasks: {stage['numTasks']}")
        if 'completionTime' in stage and 'submissionTime' in stage:
            completion_time = datetime.strptime(stage['completionTime'], '%Y-%m-%dT%H:%M:%S.%fGMT')
            submission_time = datetime.strptime(stage['submissionTime'], '%Y-%m-%dT%H:%M:%S.%fGMT')
            duration = (completion_time - submission_time).total_seconds() * 1000
            report.append(f"  - Duration: {duration:.2f} ms")
        report.append("")

    for stage in stages:
        print_stage(stage)

    # 3. Performance and Data Efficiency Analysis
    report.append("3. Performance and Data Efficiency Analysis:")
    total_duration = sum(
        (datetime.strptime(stage['completionTime'], '%Y-%m-%dT%H:%M:%S.%fGMT') -
         datetime.strptime(stage['submissionTime'], '%Y-%m-%dT%H:%M:%S.%fGMT')).total_seconds() * 1000
        for stage in stages if 'completionTime' in stage and 'submissionTime' in stage
    )
    
    total_shuffle_read = sum(executor['totalShuffleRead'] for executor in executors)
    total_shuffle_write = sum(executor['totalShuffleWrite'] for executor in executors)
    
    total_peak_execution_memory = sum(executor['memoryMetrics'].get('usedOnHeapStorageMemory', 0) for executor in executors)

    # Summing up the total executor CPU time and executor run time
    total_executor_run_time = sum(stage.get('executorRunTime', 0) for stage in stages)
    total_executor_cpu_time = sum(stage.get('executorCpuTime', 0) for stage in stages)
    total_executor_deserialize_time = sum(stage.get('executorDeserializeTime', 0) for stage in stages)
    total_result_serialization_time = sum(stage.get('resultSerializationTime', 0) for stage in stages)
    total_fetch_wait_time = sum(stage.get('fetchWaitTime', 0) for stage in stages)

    # Input/Output Metrics
    total_bytes_read = sum(stage.get('bytesRead', 0) for stage in stages)
    total_bytes_written = sum(stage.get('bytesWritten', 0) for stage in stages)
    total_records_read = sum(stage.get('recordsRead', 0) for stage in stages)
    total_records_written = sum(stage.get('recordsWritten', 0) for stage in stages)

    # Shuffle Metrics
    total_shuffle_read_time = sum(stage.get('shuffleReadTime', 0) for stage in stages)
    total_shuffle_write_time = sum(stage.get('shuffleWriteTime', 0) for stage in stages)
    total_shuffle_records_read = sum(stage.get('shuffleRecordsRead', 0) for stage in stages)
    total_shuffle_records_written = sum(stage.get('shuffleRecordsWritten', 0) for stage in stages)

    # Network Metrics
    total_remote_blocks_fetched = sum(stage.get('remoteBlocksFetched', 0) for stage in stages)
    total_local_blocks_fetched = sum(stage.get('localBlocksFetched', 0) for stage in stages)

    # JVM Metrics
    total_jvm_heap_used = sum(executor.get('jvmHeapUsed', 0) for executor in executors)
    total_jvm_off_heap_used = sum(executor.get('jvmOffHeapUsed', 0) for executor in executors)

    report.append(f"- Total duration: {total_duration:.2f} ms")
    report.append(f"- Total shuffle read: {total_shuffle_read} bytes")
    report.append(f"- Total shuffle write: {total_shuffle_write} bytes")
    report.append(f"- Total executor run time: {total_executor_run_time} ms")
    report.append(f"- Total executor CPU time: {total_executor_cpu_time} ns")
    report.append(f"- Total executor deserialize time: {total_executor_deserialize_time} ms")
    report.append(f"- Total result serialization time: {total_result_serialization_time} ms")
    report.append(f"- Total fetch wait time: {total_fetch_wait_time} ms")
    report.append(f"- Total peak execution memory: {total_peak_execution_memory} bytes")
    report.append(f"- Total bytes read: {total_bytes_read} bytes")
    report.append(f"- Total bytes written: {total_bytes_written} bytes")
    report.append(f"- Total records read: {total_records_read}")
    report.append(f"- Total records written: {total_records_written}")
    report.append(f"- Total shuffle read time: {total_shuffle_read_time} ms")
    report.append(f"- Total shuffle write time: {total_shuffle_write_time} ms")
    report.append(f"- Total shuffle records read: {total_shuffle_records_read}")
    report.append(f"- Total shuffle records written: {total_shuffle_records_written}")
    report.append(f"- Total remote blocks fetched: {total_remote_blocks_fetched}")
    report.append(f"- Total local blocks fetched: {total_local_blocks_fetched}")
    report.append(f"- Total JVM heap used: {total_jvm_heap_used} bytes")
    report.append(f"- Total JVM off-heap used: {total_jvm_off_heap_used} bytes")
    report.append("")



    # 7. Memory Usage
    report.append("7. Memory Usage:")
    total_memory_used = sum(executor['memoryMetrics']['usedOnHeapStorageMemory'] for executor in executors)
    total_disk_used = sum(executor['diskUsed'] for executor in executors)

    report.append(f"- Total memory used: {total_memory_used} bytes")
    report.append(f"- Total disk used: {total_disk_used} bytes")
    report.append("")

    # 8. Garbage Collection
    report.append("8. Garbage Collection:")
    total_gc_time = sum(executor['totalGCTime'] for executor in executors)

    report.append(f"- Total GC time: {total_gc_time} ms")
    report.append("")



    # 10. Job Metrics
    report.append("10. Job Metrics:")
    total_jobs = len(jobs)
    total_job_duration = sum(
        (datetime.strptime(job['completionTime'], '%Y-%m-%dT%H:%M:%S.%fGMT') -
         datetime.strptime(job['submissionTime'], '%Y-%m-%dT%H:%M:%S.%fGMT')).total_seconds() * 1000
        for job in jobs if 'completionTime' in job and 'submissionTime' in job
    )
    total_failed_tasks = sum(stage.get('numFailedTasks', 0) for stage in stages)
    total_speculative_tasks = sum(stage.get('numSpeculativeTasks', 0) for stage in stages)
    total_errors = sum(executor.get('errorCount', 0) for executor in executors)

    report.append(f"- Total jobs: {total_jobs}")
    report.append(f"- Total job duration: {total_job_duration:.2f} ms")
    report.append(f"- Total failed tasks: {total_failed_tasks}")
    report.append(f"- Total speculative tasks: {total_speculative_tasks}")
    report.append(f"- Total errors: {total_errors}")
    report.append("")


    return "\n".join(report)

# Paths to JSON input files
# details_path = '/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/dijkstras/analysis_data/spark_ui_data_part2/local-1719702930792/details.json'
# executors_path = '/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/dijkstras/analysis_data/spark_ui_data_part2/local-1719702930792/executors.json'
# storage_rdd_path = '/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/dijkstras/analysis_data/spark_ui_data_part2/local-1719702930792/storage_rdd.json'
# stages_path = '/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/dijkstras/analysis_data/spark_ui_data_part2/local-1719702930792/stages.json'
# jobs_path = '/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/dijkstras/analysis_data/spark_ui_data_part2/local-1719702930792/jobs.json'

details_path = '/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/pagerank/analysis_data/spark_ui_data_part3/local-1719918169214/details.json'
executors_path = '/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/pagerank/analysis_data/spark_ui_data_part3/local-1719918169214/executors.json'
storage_rdd_path = '/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/pagerank/analysis_data/spark_ui_data_part3/local-1719918169214/storage_rdd.json'
stages_path = '/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/pagerank/analysis_data/spark_ui_data_part3/local-1719918169214/stages.json'
jobs_path = '/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/pagerank/analysis_data/spark_ui_data_part3/local-1719918169214/jobs.json'



# Read JSON input files and parse data
with open(details_path) as f:
    details = json.load(f)
with open(executors_path) as f:
    executors = json.load(f)
with open(storage_rdd_path) as f:
    storage_rdd = json.load(f)
with open(stages_path) as f:
    stages = json.load(f)
with open(jobs_path) as f:
    jobs = json.load(f)

# Generate the comprehensive analysis report
comprehensive_analysis_report = generate_comprehensive_analysis_report(details, executors, storage_rdd, stages, jobs)

# Save the comprehensive report to a text file
# comprehensive_output_file_path = '/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/dijkstras/analysis_data/code2_analysis_report_comprehensive.txt'
comprehensive_output_file_path = '/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_3/src/pagerank/analysis_data/code3_analysis_report_comprehensive.txt'
with open(comprehensive_output_file_path, 'w') as file:
    file.write(comprehensive_analysis_report)

print(f"Comprehensive analysis report saved to {comprehensive_output_file_path}")
