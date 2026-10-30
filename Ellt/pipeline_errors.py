class BatchLoadError(RuntimeError):
    def __init__(
        self,
        message: str,
        successful_batches: int,
        failed_batches: int,
    ) -> None:
        super().__init__(message)
        self.successful_batches = successful_batches
        self.failed_batches = failed_batches
