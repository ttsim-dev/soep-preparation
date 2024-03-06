"""All the general configuration of the project."""
from pathlib import Path

SRC = Path(__file__).parent.resolve()
BLD = SRC.joinpath("..", "..", "bld").resolve()

TEST_DIR = SRC.joinpath("..", "..", "tests").resolve()
PAPER_DIR = SRC.joinpath("..", "..", "paper").resolve()

GROUPS = ["marital_status", "qualification"]

CATEGORIES_TO_REMOVE = ["[-8] Frage in diesem Jahr nicht Teil des Frageprogramms", "[-5] in Fragebogenversion nicht enthalten", "[-3] nicht valide", "[-2] trifft nicht zu", "[-1] keine Angabe"]

__all__ = ["BLD", "SRC", "TEST_DIR", "GROUPS", "CATEGORIES_TO_REMOVE"]
