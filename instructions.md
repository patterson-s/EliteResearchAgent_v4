# Prosopography database operations

We are working with a db with multiple schemas. One of them is for "organizations"

Right now, there are many organizations that have corpus members - meaning we track someone in that org. 

However, for most of the organizations, we don't have a "location" variable. Ideally - we would have each organization with the 
- city, country, and region

First, study the db structure for organizations. 
Second, think through how we could identify the location of different organizations using the serper API, and the RAG-type, reranking approach that we used earlier. Use Cohere for the LLM. 

We should keep the sources. We want to try to validate the location in more than one source if possible. 
