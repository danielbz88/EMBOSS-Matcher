import sys
import time
from csv import reader, writer
from typing import List
import re

from embos import serviceRun, getResult


def _run_job(a_sequence: str, b_sequence: str, s_type: str = 'protein', email: str = 'carina.benzvi@gmail.com'):

    # Submit the job
    job_id = serviceRun(
        email=email,
        title=None,
        params={
            'asequence': a_sequence,
            'bsequence': b_sequence,
            'stype': s_type
        }
    )

    # Sync mode
    print("JobId: " + job_id, file=sys.stderr)
    time.sleep(3)
    return getResult(jobId=job_id, is_write_to_file=False)


def create_output_row(gliadin_name: str, gliadin_sequence: str, epitope: str, parent_protein: str, result: str):
    length = re.search('Length:\s+([0-9]*)', result).group(1)
    identity = re.search('Identity:\s+([0-9/]*) ', result).group(1)
    identity_percentage = re.search('Identity:\s+[0-9/]*\s+\(([0-9\.]*)%\)', result).group(1)
    similarity = re.search('Similarity:\s+([0-9/]*) ', result).group(1)
    similarity_percentage = re.search('Similarity:\s+[0-9/]*\s+\(([0-9\.]*)%\)', result).group(1)
    gaps = re.search('Gaps:\s+([0-9/]*) ', result).group(1)
    score = re.search('Score:\s+([0-9]*)', result).group(1)

    seq_1_start = re.search('=======================================\n\n\s+([0-9]*)', result).group(1)
    seq_1_end = re.search('=======================================\n\n\s+[0-9]*\s+([0-9]*)', result).group(1)
    sequence_1 = re.search('=======================================\n\n.*\n.*\s([A-Z]+)', result).group(1)

    sequence_2 = re.search('=======================================\n\n.*\n.*\n.*\n.*\s([A-Z-]+)', result).group(1)
    seq_2_start = re.search('=======================================\n\n.*\n.*\n.*\n.*\n\s+([0-9]+)', result).group(1)
    seq_2_end = re.search('=======================================\n\n.*\n.*\n.*\n.*\n\s+[0-9]+\s+([0-9]*)', result).group(1)
    return [gliadin_name, gliadin_sequence, epitope, parent_protein, sequence_1, seq_1_start, seq_1_end, sequence_2,
            seq_2_start, seq_2_end, length, identity, identity_percentage, similarity, similarity_percentage, gaps,
            score]


def run(input_filepath: str, output_filepath: str):
    output_csv_header = ['Gliadin Name', 'Gliadin Sequence', 'Epitope', 'Parent Protein', 'Sequence 1', 'Start', 'End',
                         'Sequence 2', 'Start', 'End', 'Length', 'Identity', 'Identity %', 'Similarity', 'Similarity %',
                         'Gaps', 'Score']
    _create_csv(filepath=output_filepath, header=output_csv_header)

    i = 0
    with open(input_filepath, 'r') as read_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = reader(read_obj)
        header = next(csv_reader)
        # Iterate over each row in the csv using reader object
        if header:
            for row in csv_reader:
                if i > 3:
                    return
                i += 1

                # row variable is a list that represents a row in csv
                gliadin_name, gliadin_sequence, epitope, parent_protein, location = row
                result = _run_job(a_sequence=gliadin_sequence, b_sequence=epitope)
                _add_row_to_csv(
                    filepath=output_filepath,
                    row=create_output_row(
                        gliadin_name=gliadin_name,
                        gliadin_sequence=gliadin_sequence,
                        epitope=epitope,
                        parent_protein=parent_protein,
                        result=result
                    )
                )


def _create_csv(filepath: str, header: List[str]):
    with open(filepath, "w", newline='') as f:
        csv_writer = writer(f, delimiter=',')
        csv_writer.writerow(header)  # write the header


def _add_row_to_csv(filepath: str, row: List):
    with open(filepath, "a+", newline='') as f:
        csv_writer = writer(f, delimiter=',')
        csv_writer.writerow(row)  # write the row


if __name__ == '__main__':
    # input_filepath = 'sample/input.csv'
    input_filepath = ''

    if not input_filepath:
        raise Exception('Go to main.py, and set the variable "input_filepath"')

    output_filepath = f'{input_filepath.split(".csv")[0]}_{"output"}.csv'

    run(input_filepath=input_filepath, output_filepath=output_filepath)
