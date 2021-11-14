# pylint: disable=no-value-for-parameter
from dagster import job, op
from dagster.utils import file_relative_path
from dagster_ge.factory import ge_data_context, ge_validation_op_factory, ge_checkpoint_op_factory
from pandas import read_csv


@op
def read_in_datafile(csv_path):
    return read_csv(csv_path)


@op
def process_payroll(df):
    return len(df)


# start_ge_demo_marker_op
@op
def postprocess_payroll(numrows, expectation, checkpoint_errors):
    if expectation["success"] & checkpoint_errors == 0:
        return numrows
    else:
        raise ValueError


# end_ge_demo_marker_op


# start_ge_demo_marker_factory
payroll_expectations = ge_validation_op_factory(
    name="ge_validation_op", datasource_name="getest", suite_name="basic.warning"
)

payroll_checkpoint = ge_checkpoint_op_factory(
    name="ge_checkpoint_op", datasource_name="getest", checkpoint_name="example_checkpoint_v2"
)
# end_ge_demo_marker_factory


# start_ge_demo_marker_job
@job(
    resource_defs={"ge_data_context": ge_data_context},
    config={
        "resources": {
            "ge_data_context": {
                "config": {"ge_root_dir": file_relative_path(__file__, "./great_expectations")}
            }
        },
        "ops": {
            "read_in_datafile": {
                "inputs": {
                    "csv_path": {"value": file_relative_path(__file__, "./data/succeed.csv")}
                }
            }
        },
    },
)
def payroll_data():
    output_df = read_in_datafile()
    expectation = payroll_expectations(output_df)
    checkpoint_errors = payroll_checkpoint()
    postprocess_payroll(process_payroll(output_df), expectation, checkpoint_errors)


# end_ge_demo_marker_job
