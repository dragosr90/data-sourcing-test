def check_deadline_violations(
    self,
    files_per_delivery_entity: list[dict[str, str]],
    log_config: ProcessLogConfig
) -> None:
    """Check for deadline violations for FINOB and NME files.
    
    Raises error if deadline has passed and expected files are missing.
    
    Args:
        files_per_delivery_entity: List of files found
        log_config: Process log configuration
        
    Raises:
        NonSSFExtractionError: If deadline violations are found
    """
    current_dt = datetime.now(tz=timezone.utc)
    
    # Get expected files for FINOB and NME from metadata with their deadlines
    finob_nme_expected = self.meta_data.filter(
        self.meta_data.SourceSystem.isin(["finob", "nme"])  # Use lowercase
    ).select("SourceFileName", "SourceSystem", "Deadline").collect()
    
    # Get delivered files for FINOB and NME
    delivered_files = {
        Path(file["file_name"]).stem: file["source_system"].lower()
        for file in files_per_delivery_entity 
        if file["source_system"].lower() in ["finob", "nme"]
    }
    
    # Check for missing files
    missing_files = []
    for row in finob_nme_expected:
        expected_file = row["SourceFileName"]
        source_system = row["SourceSystem"]
        deadline = row["Deadline"]
        
        if expected_file not in delivered_files:
            # Convert deadline to datetime
            if deadline:
                if isinstance(deadline, str):
                    deadline_dt = datetime.strptime(deadline, "%Y-%m-%d").replace(
                        tzinfo=timezone.utc
                    )
                else:
                    deadline_dt = datetime.combine(
                        deadline, datetime.min.time()
                    ).replace(tzinfo=timezone.utc)
                
                # Only report violation if deadline has passed
                if current_dt >= deadline_dt:
                    deadline_str = deadline_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                    missing_files.append({
                        "file": f"{source_system}/{expected_file}",
                        "deadline": deadline_str,
                        "source_system": source_system
                    })
                    logger.error(
                        f"Deadline passed ({deadline_str}): Missing expected file "
                        f"{expected_file} from {source_system}"
                    )
    
    if missing_files:
        # Create error message with all missing files
        missing_files_list = [
            f"{mf['file']} (deadline: {mf['deadline']})" 
            for mf in missing_files
        ]
        error_msg = (
            f"Deadline violation: Missing files after deadline - "
            f"{', '.join(missing_files_list)}"
        )
        
        # Log the error for each missing file's source system
        for missing_file_info in missing_files:
            source_system = missing_file_info['source_system']
            file_path = missing_file_info['file']
            deadline = missing_file_info['deadline']
            
            # Update log metadata with deadline information
            self.update_log_metadata(
                source_system=source_system.upper(),
                key=Path(file_path.split('/')[1]).stem,
                file_delivery_status=NonSSFStepStatus.RECEIVED,
                result="ERROR",
                comment=f"Deadline passed ({deadline}): Missing expected file {file_path.upper()}",
            )
            
            self._append_to_process_log(
                **log_config,
                source_system=source_system.upper(),  # Convert back to uppercase for logging
                comments=f"Deadline passed ({deadline}): Missing expected file {file_path.upper()}",
                status="Failed",
            )
        
        # Raise error to stop the pipeline
        raise NonSSFExtractionError(
            NonSSFStepStatus.RECEIVED,
            additional_info=error_msg
        )
