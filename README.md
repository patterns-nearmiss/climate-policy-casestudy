# climate-policy-casestudy
Code, data, and documentation to reproduce the analyses from our Patterns manuscript on near-miss events and their policy implications.
# Nearmiss Policy Patterns
---

## Repository Contents

nearmiss-policy-patterns/
├── twins_sdg13_export.csv
└── export_code_notebook.html



1. **`twins_sdg13_export.csv`**  
   - A table of paired articles on semantic similarity focused on SDG 13 (“Climate Action”).
     The pairs were created by using miniLM + KNN cosine similarity to find pairs of highly similar climate change related papers where one is cited in policy and the other is not. Matched by similarity content, within 1 year publication date apart, and no common authors. 
     
   - Columns include:  
     - `pair_id` - a unique id for the specific pair of matched papers 
     - `scopus_id` - the scopus id of the specific paper. 
     - `CiteScore` - the CiteScore (journal impact score) of the citing 
     - `academic_citations(adjusted)` - number of academic citations of the paper accrued before it was first cited in policy
     - `max_h_index` - the highest h-index of the authoring team. 
     - `collaboration_global_south` - binary score if paper includes at least one co-author based at a global south institution. 
     - `collaboration_withGovt_authors`  - binary score if paper includes at least one co-author that is affiliated with a government instituion.
     - `media_mentions`  - number of media mentions (based on PlumX)
     - `cited_in_policy`  - binary if paper is cited in policy or not
     - `team_past_policy_impact`  - cummulative total prior policy citations of the authoring team.
     - `doi`  - unique id of the document.

2. **`export_code_notebook.html`**  
   - An export of the Databricks notebook (HTML format) containing all data-processing and analysis code.  
   - To inspect or run the code, open this file in any web browser. It shows each code cell, output, and markdown from the original Databricks notebook.The code includes two parts. Dataprocessing using Python/PySpark, and R notebook for the regression analysis. 

---

## How to Use This Repository

1. **Download or clone this repo**  
   ```bash
   git clone https://github.com/patterns-nearmiss/nearmiss-policy-patterns.git
   cd nearmiss-policy-patterns
