from typing import Protocol


class PipelineStage(Protocol):
    def run(self, input_files: list[str], output_dir: str) -> list[str]:
        """
        Process a single part of the input data.

        Args:
            input_files (list[str]): List of input file paths or URIs for a single part.
            output_dir (str): Directory where the output files should be written.

        Returns:
            list[str]: List of output file paths or URIs for the processed part.
        """

    def runall(self, input_pfiles: list[list[str]], output_dir: str) -> list[list[str]]:
        """
        Process all parts of the input data.

        Args:
            input_pfiles (list[list[str]]): List of lists of input file paths or URIs.
            output_dir (str): Directory where the output files should be written.

        Returns:
            list[list[str]]: List of lists of output file paths or URIs for all processed parts.
        """
