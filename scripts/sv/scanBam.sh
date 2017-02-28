#!/bin/bash

set -eu

if [[ -z ${GATK_DIR+x} || -z ${CLUSTER_NAME+x} || -z ${MASTER_NODE+x} || -z ${PROJECT_OUTPUT_DIR+x} ]]; then 
    if [[ "$#" -ne 3 ]]; then
        echo "Please provide: local directory of GATK build, cluster name, output dir on the cluster"
    exit 1
    fi
    GATK_DIR=$1
    CLUSTER_NAME=$2
    OUTPUT_DIR=$3
    MASTER_NODE="hdfs://""$CLUSTER_NAME""-m:8020"
    PROJECT_OUTPUT_DIR="$MASTER_NODE"/"$OUTPUT_DIR"
fi

REFERENCE_LOCATION=/reference/Homo_sapiens_assembly19.fasta.64.img
INPUT_BAM="$MASTER_NODE"/data/NA12878_PCR-_30X.bam
SKIP_INTERVAL_LIST="$MASTER_NODE"/reference/GRCh37.kill.intervals

echo "Assuming reference image: " "$REFERENCE_LOCATION"
echo "Assuming input bam: " "$INPUT_BAM"
echo "Assuming reference skip list: " "$SKIP_INTERVAL_LIST"

cd "$GATK_DIR" 

./gatk-launch FindBreakpointEvidenceSpark \
    -I "$INPUT_BAM" \
    -O "$PROJECT_OUTPUT_DIR"/aligned_assembly_contigs.sam \
    --alignerIndexImage "$REFERENCE_LOCATION" \
    --exclusionIntervals "$SKIP_INTERVAL_LIST" \
    --kmersToIgnore "$MASTER_NODE"/reference/Homo_sapiens_assembly38.dups \
    --breakpointIntervals "$PROJECT_OUTPUT_DIR"/intervals \
    -- \
    --sparkRunner GCS \
    --cluster "$CLUSTER_NAME" \
    --num-executors 20 \
    --driver-memory 30G \
    --executor-memory 30G \
    --conf spark.yarn.executor.memoryOverhead=5000
