from dataclasses import dataclass
from enum import Enum
import os
import subprocess
import requests
import shutil
from pathlib import Path
import typing
import typing_extensions

from latch.resources.workflow import workflow
from latch.resources.tasks import nextflow_runtime_task, custom_task
from latch.types.file import LatchFile
from latch.types.directory import LatchDir, LatchOutputDir
from latch.ldata.path import LPath
from latch_cli.nextflow.workflow import get_flag
from latch_cli.nextflow.utils import _get_execution_name
from latch_cli.utils import urljoins
from latch.types import metadata
from flytekit.core.annotation import FlyteAnnotation

from latch_cli.services.register.utils import import_module_by_path

meta = Path("latch_metadata") / "__init__.py"
import_module_by_path(meta)
import latch_metadata

@custom_task(cpu=0.25, memory=0.5, storage_gib=1)
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    headers = {"Authorization": f"Latch-Execution-Token {token}"}

    print("Provisioning shared storage volume... ", end="")
    resp = requests.post(
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage",
        headers=headers,
        json={
            "storage_gib": 100,
        }
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]






@nextflow_runtime_task(cpu=4, memory=8, storage_gib=100)
def nextflow_runtime(pvc_name: str, input: typing.Optional[LatchFile], platform: typing.Optional[str], protocol: typing.Optional[str], outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})], email: typing.Optional[str], genome: typing.Optional[str], fasta: typing.Optional[LatchFile], gff: typing.Optional[LatchFile], bowtie2_index: typing.Optional[str], primer_bed: typing.Optional[LatchFile], primer_fasta: typing.Optional[LatchFile], primer_set: typing.Optional[str], primer_set_version: typing.Optional[float], save_reference: typing.Optional[bool], fastq_dir: typing.Optional[LatchDir], fast5_dir: typing.Optional[LatchDir], sequencing_summary: typing.Optional[LatchFile], artic_scheme: typing.Optional[str], artic_minion_medaka_model: typing.Optional[str], skip_pycoqc: typing.Optional[bool], skip_nanoplot: typing.Optional[bool], nextclade_dataset: typing.Optional[str], nextclade_dataset_name: typing.Optional[str], nextclade_dataset_reference: typing.Optional[str], nextclade_dataset_tag: typing.Optional[str], skip_mosdepth: typing.Optional[bool], skip_pangolin: typing.Optional[bool], skip_nextclade: typing.Optional[bool], skip_asciigenome: typing.Optional[bool], skip_variants_quast: typing.Optional[bool], skip_variants_long_table: typing.Optional[bool], skip_multiqc: typing.Optional[bool], kraken2_variants_host_filter: typing.Optional[bool], save_trimmed_fail: typing.Optional[bool], skip_fastqc: typing.Optional[bool], skip_kraken2: typing.Optional[bool], skip_fastp: typing.Optional[bool], skip_cutadapt: typing.Optional[bool], variant_caller: typing.Optional[str], ivar_trim_noprimer: typing.Optional[bool], ivar_trim_offset: typing.Optional[int], filter_duplicates: typing.Optional[bool], save_unaligned: typing.Optional[bool], save_mpileup: typing.Optional[bool], skip_ivar_trim: typing.Optional[bool], skip_picard_metrics: typing.Optional[bool], skip_snpeff: typing.Optional[bool], skip_consensus_plots: typing.Optional[bool], skip_consensus: typing.Optional[bool], skip_variants: typing.Optional[bool], spades_hmm: typing.Optional[LatchFile], blast_db: typing.Optional[str], skip_bandage: typing.Optional[bool], skip_blast: typing.Optional[bool], skip_abacas: typing.Optional[bool], skip_assembly_quast: typing.Optional[bool], skip_assembly: typing.Optional[bool], primer_left_suffix: typing.Optional[str], primer_right_suffix: typing.Optional[str], min_barcode_reads: typing.Optional[int], min_guppyplex_reads: typing.Optional[int], artic_minion_caller: typing.Optional[str], artic_minion_aligner: typing.Optional[str], asciigenome_read_depth: typing.Optional[int], asciigenome_window_size: typing.Optional[int], kraken2_db: typing.Optional[str], kraken2_db_name: typing.Optional[str], kraken2_assembly_host_filter: typing.Optional[bool], consensus_caller: typing.Optional[str], min_mapped_reads: typing.Optional[int], skip_markduplicates: typing.Optional[bool], assemblers: typing.Optional[str], spades_mode: typing.Optional[str], skip_plasmidid: typing.Optional[bool]) -> None:
    try:
        shared_dir = Path("/nf-workdir")



        ignore_list = [
            "latch",
            ".latch",
            "nextflow",
            ".nextflow",
            "work",
            "results",
            "miniconda",
            "anaconda3",
            "mambaforge",
        ]

        shutil.copytree(
            Path("/root"),
            shared_dir,
            ignore=lambda src, names: ignore_list,
            ignore_dangling_symlinks=True,
            dirs_exist_ok=True,
        )

        cmd = [
            "/root/nextflow",
            "run",
            str(shared_dir / "main.nf"),
            "-work-dir",
            str(shared_dir),
            "-profile",
            "docker",
            "-c",
            "latch.config",
                *get_flag('input', input),
                *get_flag('platform', platform),
                *get_flag('protocol', protocol),
                *get_flag('outdir', outdir),
                *get_flag('email', email),
                *get_flag('genome', genome),
                *get_flag('fasta', fasta),
                *get_flag('gff', gff),
                *get_flag('bowtie2_index', bowtie2_index),
                *get_flag('primer_bed', primer_bed),
                *get_flag('primer_fasta', primer_fasta),
                *get_flag('primer_set', primer_set),
                *get_flag('primer_set_version', primer_set_version),
                *get_flag('primer_left_suffix', primer_left_suffix),
                *get_flag('primer_right_suffix', primer_right_suffix),
                *get_flag('save_reference', save_reference),
                *get_flag('fastq_dir', fastq_dir),
                *get_flag('fast5_dir', fast5_dir),
                *get_flag('sequencing_summary', sequencing_summary),
                *get_flag('min_barcode_reads', min_barcode_reads),
                *get_flag('min_guppyplex_reads', min_guppyplex_reads),
                *get_flag('artic_minion_caller', artic_minion_caller),
                *get_flag('artic_minion_aligner', artic_minion_aligner),
                *get_flag('artic_scheme', artic_scheme),
                *get_flag('artic_minion_medaka_model', artic_minion_medaka_model),
                *get_flag('skip_pycoqc', skip_pycoqc),
                *get_flag('skip_nanoplot', skip_nanoplot),
                *get_flag('nextclade_dataset', nextclade_dataset),
                *get_flag('nextclade_dataset_name', nextclade_dataset_name),
                *get_flag('nextclade_dataset_reference', nextclade_dataset_reference),
                *get_flag('nextclade_dataset_tag', nextclade_dataset_tag),
                *get_flag('asciigenome_read_depth', asciigenome_read_depth),
                *get_flag('asciigenome_window_size', asciigenome_window_size),
                *get_flag('skip_mosdepth', skip_mosdepth),
                *get_flag('skip_pangolin', skip_pangolin),
                *get_flag('skip_nextclade', skip_nextclade),
                *get_flag('skip_asciigenome', skip_asciigenome),
                *get_flag('skip_variants_quast', skip_variants_quast),
                *get_flag('skip_variants_long_table', skip_variants_long_table),
                *get_flag('skip_multiqc', skip_multiqc),
                *get_flag('kraken2_db', kraken2_db),
                *get_flag('kraken2_db_name', kraken2_db_name),
                *get_flag('kraken2_variants_host_filter', kraken2_variants_host_filter),
                *get_flag('kraken2_assembly_host_filter', kraken2_assembly_host_filter),
                *get_flag('save_trimmed_fail', save_trimmed_fail),
                *get_flag('skip_fastqc', skip_fastqc),
                *get_flag('skip_kraken2', skip_kraken2),
                *get_flag('skip_fastp', skip_fastp),
                *get_flag('skip_cutadapt', skip_cutadapt),
                *get_flag('variant_caller', variant_caller),
                *get_flag('consensus_caller', consensus_caller),
                *get_flag('min_mapped_reads', min_mapped_reads),
                *get_flag('ivar_trim_noprimer', ivar_trim_noprimer),
                *get_flag('ivar_trim_offset', ivar_trim_offset),
                *get_flag('filter_duplicates', filter_duplicates),
                *get_flag('save_unaligned', save_unaligned),
                *get_flag('save_mpileup', save_mpileup),
                *get_flag('skip_ivar_trim', skip_ivar_trim),
                *get_flag('skip_markduplicates', skip_markduplicates),
                *get_flag('skip_picard_metrics', skip_picard_metrics),
                *get_flag('skip_snpeff', skip_snpeff),
                *get_flag('skip_consensus_plots', skip_consensus_plots),
                *get_flag('skip_consensus', skip_consensus),
                *get_flag('skip_variants', skip_variants),
                *get_flag('assemblers', assemblers),
                *get_flag('spades_mode', spades_mode),
                *get_flag('spades_hmm', spades_hmm),
                *get_flag('blast_db', blast_db),
                *get_flag('skip_bandage', skip_bandage),
                *get_flag('skip_blast', skip_blast),
                *get_flag('skip_abacas', skip_abacas),
                *get_flag('skip_plasmidid', skip_plasmidid),
                *get_flag('skip_assembly_quast', skip_assembly_quast),
                *get_flag('skip_assembly', skip_assembly)
        ]

        print("Launching Nextflow Runtime")
        print(' '.join(cmd))
        print(flush=True)

        env = {
            **os.environ,
            "NXF_HOME": "/root/.nextflow",
            "NXF_OPTS": "-Xms2048M -Xmx8G -XX:ActiveProcessorCount=4",
            "K8S_STORAGE_CLAIM_NAME": pvc_name,
            "NXF_DISABLE_CHECK_LATEST": "true",
        }
        subprocess.run(
            cmd,
            env=env,
            check=True,
            cwd=str(shared_dir),
        )
    finally:
        print()

        nextflow_log = shared_dir / ".nextflow.log"
        if nextflow_log.exists():
            name = _get_execution_name()
            if name is None:
                print("Skipping logs upload, failed to get execution name")
            else:
                remote = LPath(urljoins("latch:///your_log_dir/nf_nf_core_viralrecon", name, "nextflow.log"))
                print(f"Uploading .nextflow.log to {remote.path}")
                remote.upload_from(nextflow_log)



@workflow(metadata._nextflow_metadata)
def nf_nf_core_viralrecon(input: typing.Optional[LatchFile], platform: typing.Optional[str], protocol: typing.Optional[str], outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})], email: typing.Optional[str], genome: typing.Optional[str], fasta: typing.Optional[LatchFile], gff: typing.Optional[LatchFile], bowtie2_index: typing.Optional[str], primer_bed: typing.Optional[LatchFile], primer_fasta: typing.Optional[LatchFile], primer_set: typing.Optional[str], primer_set_version: typing.Optional[float], save_reference: typing.Optional[bool], fastq_dir: typing.Optional[LatchDir], fast5_dir: typing.Optional[LatchDir], sequencing_summary: typing.Optional[LatchFile], artic_scheme: typing.Optional[str], artic_minion_medaka_model: typing.Optional[str], skip_pycoqc: typing.Optional[bool], skip_nanoplot: typing.Optional[bool], nextclade_dataset: typing.Optional[str], nextclade_dataset_name: typing.Optional[str], nextclade_dataset_reference: typing.Optional[str], nextclade_dataset_tag: typing.Optional[str], skip_mosdepth: typing.Optional[bool], skip_pangolin: typing.Optional[bool], skip_nextclade: typing.Optional[bool], skip_asciigenome: typing.Optional[bool], skip_variants_quast: typing.Optional[bool], skip_variants_long_table: typing.Optional[bool], skip_multiqc: typing.Optional[bool], kraken2_variants_host_filter: typing.Optional[bool], save_trimmed_fail: typing.Optional[bool], skip_fastqc: typing.Optional[bool], skip_kraken2: typing.Optional[bool], skip_fastp: typing.Optional[bool], skip_cutadapt: typing.Optional[bool], variant_caller: typing.Optional[str], ivar_trim_noprimer: typing.Optional[bool], ivar_trim_offset: typing.Optional[int], filter_duplicates: typing.Optional[bool], save_unaligned: typing.Optional[bool], save_mpileup: typing.Optional[bool], skip_ivar_trim: typing.Optional[bool], skip_picard_metrics: typing.Optional[bool], skip_snpeff: typing.Optional[bool], skip_consensus_plots: typing.Optional[bool], skip_consensus: typing.Optional[bool], skip_variants: typing.Optional[bool], spades_hmm: typing.Optional[LatchFile], blast_db: typing.Optional[str], skip_bandage: typing.Optional[bool], skip_blast: typing.Optional[bool], skip_abacas: typing.Optional[bool], skip_assembly_quast: typing.Optional[bool], skip_assembly: typing.Optional[bool], primer_left_suffix: typing.Optional[str] = '_LEFT', primer_right_suffix: typing.Optional[str] = '_RIGHT', min_barcode_reads: typing.Optional[int] = 100, min_guppyplex_reads: typing.Optional[int] = 10, artic_minion_caller: typing.Optional[str] = 'nanopolish', artic_minion_aligner: typing.Optional[str] = 'minimap2', asciigenome_read_depth: typing.Optional[int] = 50, asciigenome_window_size: typing.Optional[int] = 50, kraken2_db: typing.Optional[str] = 's3://ngi-igenomes/test-data/viralrecon/kraken2_human.tar.gz', kraken2_db_name: typing.Optional[str] = 'human', kraken2_assembly_host_filter: typing.Optional[bool] = True, consensus_caller: typing.Optional[str] = 'bcftools', min_mapped_reads: typing.Optional[int] = 1000, skip_markduplicates: typing.Optional[bool] = True, assemblers: typing.Optional[str] = 'spades', spades_mode: typing.Optional[str] = 'rnaviral', skip_plasmidid: typing.Optional[bool] = True) -> None:
    """
    nf-core/viralrecon

    Sample Description
    """

    pvc_name: str = initialize()
    nextflow_runtime(pvc_name=pvc_name, input=input, platform=platform, protocol=protocol, outdir=outdir, email=email, genome=genome, fasta=fasta, gff=gff, bowtie2_index=bowtie2_index, primer_bed=primer_bed, primer_fasta=primer_fasta, primer_set=primer_set, primer_set_version=primer_set_version, primer_left_suffix=primer_left_suffix, primer_right_suffix=primer_right_suffix, save_reference=save_reference, fastq_dir=fastq_dir, fast5_dir=fast5_dir, sequencing_summary=sequencing_summary, min_barcode_reads=min_barcode_reads, min_guppyplex_reads=min_guppyplex_reads, artic_minion_caller=artic_minion_caller, artic_minion_aligner=artic_minion_aligner, artic_scheme=artic_scheme, artic_minion_medaka_model=artic_minion_medaka_model, skip_pycoqc=skip_pycoqc, skip_nanoplot=skip_nanoplot, nextclade_dataset=nextclade_dataset, nextclade_dataset_name=nextclade_dataset_name, nextclade_dataset_reference=nextclade_dataset_reference, nextclade_dataset_tag=nextclade_dataset_tag, asciigenome_read_depth=asciigenome_read_depth, asciigenome_window_size=asciigenome_window_size, skip_mosdepth=skip_mosdepth, skip_pangolin=skip_pangolin, skip_nextclade=skip_nextclade, skip_asciigenome=skip_asciigenome, skip_variants_quast=skip_variants_quast, skip_variants_long_table=skip_variants_long_table, skip_multiqc=skip_multiqc, kraken2_db=kraken2_db, kraken2_db_name=kraken2_db_name, kraken2_variants_host_filter=kraken2_variants_host_filter, kraken2_assembly_host_filter=kraken2_assembly_host_filter, save_trimmed_fail=save_trimmed_fail, skip_fastqc=skip_fastqc, skip_kraken2=skip_kraken2, skip_fastp=skip_fastp, skip_cutadapt=skip_cutadapt, variant_caller=variant_caller, consensus_caller=consensus_caller, min_mapped_reads=min_mapped_reads, ivar_trim_noprimer=ivar_trim_noprimer, ivar_trim_offset=ivar_trim_offset, filter_duplicates=filter_duplicates, save_unaligned=save_unaligned, save_mpileup=save_mpileup, skip_ivar_trim=skip_ivar_trim, skip_markduplicates=skip_markduplicates, skip_picard_metrics=skip_picard_metrics, skip_snpeff=skip_snpeff, skip_consensus_plots=skip_consensus_plots, skip_consensus=skip_consensus, skip_variants=skip_variants, assemblers=assemblers, spades_mode=spades_mode, spades_hmm=spades_hmm, blast_db=blast_db, skip_bandage=skip_bandage, skip_blast=skip_blast, skip_abacas=skip_abacas, skip_plasmidid=skip_plasmidid, skip_assembly_quast=skip_assembly_quast, skip_assembly=skip_assembly)

