#!/bin/bash

for cancer in BLCA  BRCA  COAD  ESCA  HNSC LGG  LIHC  LUAD  LUSC  OV  PAAD  READ  SARC  SKCM  STAD  TGCT; do
  echo $cancer
  python ./count_pairs.py $cancer
done
