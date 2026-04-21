#!/usr/bin/env bash

echo "=================================================="
echo "    AWS EKS & EFS Cleanup"
echo "=================================================="
echo "WARNING: This will permanently delete your cluster, storage, and all data!"
read -p "Are you sure? (y/N): " CONFIRM
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "Aborting."
    exit 1
fi

echo "1. Finding EFS resources..."
VPC_ID=$(aws eks describe-cluster --name spark-benchmark-cluster --region us-east-1 --query "cluster.resourcesVpcConfig.vpcId" --output text 2>/dev/null || echo "None")
EFS_ID=$(aws efs describe-file-systems --query "FileSystems[?Name=='spark-benchmark-efs'].FileSystemId" --output text 2>/dev/null || echo "")

if [ -n "$EFS_ID" ]; then
    echo "2. Deleting EFS Mount Targets..."
    TARGETS=$(aws efs describe-mount-targets --file-system-id $EFS_ID --query "MountTargets[*].MountTargetId" --output text)
    for T in $TARGETS; do
        aws efs delete-mount-target --mount-target-id $T
    done
    
    echo "Waiting for mount targets to delete..."
    sleep 30
    
    echo "3. Deleting EFS File System..."
    aws efs delete-file-system --file-system-id $EFS_ID
fi

if [ "$VPC_ID" != "None" ]; then
    echo "4. Deleting EFS Security Group..."
    SG_ID=$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=efs-sg-spark" --query "SecurityGroups[0].GroupId" --output text 2>/dev/null || echo "None")
    if [ "$SG_ID" != "None" ] && [ -n "$SG_ID" ]; then
        aws ec2 delete-security-group --group-id $SG_ID
    fi
fi

echo "5. Deleting EKS cluster..."
eksctl delete cluster --name spark-benchmark-cluster --region us-east-1

echo "=================================================="
echo "    Cleanup Complete!"
echo "=================================================="
