# Merger

Changes:
1. No need to type full document name: choose from the list of *.xslx files in the directory
2. Changed char.isalpha() and char.isdigit() for r'[^\p{L}\p{N}]+' to support any language
3. Added decoder unidecode() for accents processing
   
