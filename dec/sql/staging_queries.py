def copy_raw_table(table, s3_path, iam_arn):
    return f"""
                COPY {table} 
                FROM '{s3_path}'
                CREDENTIALS 'aws_iam_role={iam_arn}'
                REGION 'us-east-1'
                CSV
                IGNOREHEADER 1;
    """
