import os

from rich.progress import Progress

from lightningdb.df import LightningCtx
from lightningdb.pipeline_stage import PipelineStage
from lightningdb.stages.fetch import Fetch
from lightningdb.stages.multifetch import MultiFetch


def has_part(ctx: LightningCtx, dfname: str, part: int) -> bool:
    return ctx.get_files(ctx, dfname, part) is not None


def run_pipeline(ctx: LightningCtx, name: str, pipeline: list[PipelineStage]):
    nparts = 1
    prev_df = None
    cur_df = None

    with Progress() as progress:
        for i, stage in enumerate(pipeline):
            # MultiFetch and Fetch stages are IO-intensive and can be skipped
            memorize = isinstance(stage, MultiFetch) or isinstance(stage, Fetch)

            cur_df = f"{name}@{i}"
            output_dir = os.path.join(ctx.repodir, cur_df)
            if hasattr(stage, "runall"):
                task_id = progress.add_task(
                    f"{cur_df} {stage.__class__.__name__}", total=1
                )
                input_pfiles = [
                    [
                        os.path.join(ctx.repodir, prev_df, file)
                        for file in ctx.get_files(prev_df, part)
                    ]
                    for part in range(nparts)
                ]
                output_pfiles = stage.runall(input_pfiles, output_dir)
                for part, files in enumerate(output_pfiles):
                    ctx.new_df(cur_df, part, files)
                nparts = len(output_pfiles)
                progress.update(task_id, advance=1)
            else:
                task_id = progress.add_task(
                    f"{cur_df} {stage.__class__.__name__}", total=nparts
                )

                for part in range(nparts):
                    if memorize and has_part(ctx, cur_df, part):
                        progress.update(task_id, advance=1)
                        continue

                    input_files = [
                        os.path.join(ctx.repodir, prev_df, file)
                        for file in ctx.get_files(prev_df, part)
                    ]

                    output_files = stage.run(input_files, output_dir)
                    ctx.new_df(cur_df, part, output_files)
                    progress.update(task_id, advance=1)

            prev_df = cur_df
