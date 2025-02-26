import sys
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from common.logs import logger
from common.utils import total_time_this
from BRNtoSLV import parser
from BRNtoSLV.controlrecord import ControlEntries
from BRNtoSLV.operation import BRNtoSLV
from db.model import create_all
from db.db_connector import DBConnector

def main(args):

    if args.model_create:
        engine = DBConnector().staging()
        create_all(engine)
        engine.dispose()
        sys.exit(0)

    controlEntries = ControlEntries(
        'StgtoDW',
        args.sources and args.sources.split(args.delimiter),
        args.groups and args.groups.split(args.delimiter),
        args.exclude_sources and args.exclude_sources.split(args.delimiter),
        args.exclude_groups and args.exclude_groups.split(args.delimiter),
        args.object_type and args.object_type.split(args.delimiter),
        args.calling_sequence and args.calling_sequence.split(args.delimiter),
        args.loadfrequency,
        args.failed,
    )
    
    records = controlEntries.fetch_records()

    if records:
        logger.info("%d entries in records", len(records))
    else:
        logger.info("No entries in records")
        sys.exit(0)

    if args.list_sources:
        for record in records:
            logger.info(f"{record.dataflowflag:<20} - {record.sourceid}")
        sys.exit(0)

    df_records = pd.DataFrame(records)

    sorted_dfTonamedtuple = df_records['sourcecallingseq'].sort_values().unique()

    run(sorted_dfTonamedtuple, df_records, args)




@total_time_this
def run(sorted_dfTonamedtuple, df_records, args):
    for i in sorted_dfTonamedtuple:
        logger.info(f'Processing calling sequence {i}')
        records = [*df_records[df_records['sourcecallingseq'] == i].itertuples(name="Record", index=False)]
        # print(records)
        # obj = BRNtoSLV(records)

        if args.parallel:
            with ThreadPoolExecutor(max_workers=args.parallel) as executor:
                # executor.map(obj.stg_to_dwh, records)
                for record in records:
                    obj = BRNtoSLV(record)
                    executor.submit(obj.stg_to_dwh)
                logger.info("Finished running staging to dwh calling sequence %d in parallel", i)
        else:
            for record in records:
                obj = BRNtoSLV(record)
                obj.stg_to_dwh()
            logger.info("Finished running staging to dwh calling sequence %d in series", i)

# def run(sorted_dfTonamedtuple, df_records, args):
#     """Runs the ETL process for each calling sequence."""
#     for i in sorted_dfTonamedtuple:
#         logger.info(f'Processing calling sequence {i}')
#         records = [
#             *df_records[df_records['sourcecallingseq'] == i].itertuples(name="Record", index=False)
#         ]

#         if args.parallel:
#             with ThreadPoolExecutor(max_workers=args.parallel) as executor:
#                 executor.map(lambda record: BRNtoSLV(record).stg_to_dwh(), records)
#                 logger.info("Finished running staging to DWH for calling sequence %d in parallel", i)
#         else:
#             for record in records:
#                 BRNtoSLV(record).stg_to_dwh()
#             logger.info("Finished running staging to DWH for calling sequence %d in series", i)

if __name__ == '__main__':
    cli_args = parser.parse_args()
    main(cli_args)