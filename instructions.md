# Prosopography database operations

## Part 1: Building central db

We are working on the prosopography project. We have been working on this project in a haphazard way, running lots of prompts, but not properly storing data in a db and not properly documenting our progress. 

Our goal is to build up the database for this project so that in future iterations, we make calls to the db, document what we are going to do, and then store the outputs back in the db in a coherent and consistent manner. 

At the centre of our db should be the "elite". Since we are researching elites, they are at the center. From the elites, we have the following links: 
- Raw data: this should already be in the db. it includes the raw search results from serper, information about the search time and conditions, the contents that were extracted from the searched links, the chunks, and the embeddings. 
- Biographical data: this is information about the elite that describes the person independent of their career-events. For example, birth date, still alive? , death date (if relevant), and nationality. In this case, we also want to store which HLP they served on, as this is at the centre of our research.  
- Career-events: Career-events can be: 
	- Education
	- Career-position
	- Award, honorific, ceremonial, etc.. 
- Provenance: 
	- All of the data from the biographical and career-events have been derived from the raw data via a series of prompts. I have kept track of the prompts that we ran to do all of this, the config information, etc.. For each element in the biographical and career-event data, we should keep information in the db that describes how the data was generated and from which sources. 

I am still new to data base structures, but I believe that this is a "star" configuration, in that the different types of data are being kept in separate areas, but I'm not worried about making everything super normalized. 

Biographical data: 
- in this folder: "C:\Users\spatt\Desktop\eliteresearchagent_v3\services\biographical\review"
- and we want the folder that ends like this for each person: "C:\Users\spatt\Desktop\eliteresearchagent_v3\services\biographical\review\Abhijit_Banerjee_bio.json"
	- that is, _bio.json

Career-events:  
- "C:\Users\spatt\Desktop\eliteresearchagent_v3\services\integrated_01\outputs", then within each person folder, one of these: "C:\Users\spatt\Desktop\eliteresearchagent_v3\services\integrated_01\outputs\Abhijit_Banerjee\Abhijit_Banerjee_career_history.json"

## Part 2: Organizations and derivatives

The database for part 1 is the central db. It is not "raw" technically, but it is the center for analytical operations downstream. 

Some of these operations include: 
- typologizing career-trajectories across individuals
- standardizing organizations across all individuals 

Organizations:
- Organizations are themselves interesting for us. We should also maintain a schema for organizations that allows us to connect various organizational properties to the places that people work. Additionally, we are interested in understanding different "types" of organizations in various ways. Finally, organizations can have different spelling/language; organizations can have different hierarchical or nested structures. We need a way to see all of this around the organizations. 

Derivatives: 
- Analytical derivatives are LLM-generated outputs that are downstream from the raw data. They are flexible - for example, they can be: 
	- About a person: for example, this person's career-trajectory is type x; this person's primary expertise is y
	- About a career-event: for example, this is a central event; this event shows y function
	- About an organization: for example, this org is at position z in the UN hierarchy; this organization is a NGO

When working with derivatives, I need to strike the right balance between easy of access and rigor
- I want to be able to quickly access derived variables for analytical purposes; however, I also want to keep track of provenance, prompts and configs, and evaluation status. It is impractical to store all of this information. 

In this sense, I think that instead of having independent schemas for each of the different derivative types, it might make more sense to reuse the existing schemas, but to have a flag for "base" or "derivative" - where the data from part 1 would be indicated as the base. 

Derivates should also have provenance information. 

## Part 3: Interaction Layer

Once the database is built, we can turn to the interaction layer. I have two ideas in mind here: 
1) Continuous development protocols
2) Production protocols

For continuous development protocols, I am thinking about the procedures for adding and improving on the db. For example, after running a new derivative round, I may want to add new variables to the db. In doing so, I want to ensure that each time I add a new variable, I am doing so with all of the provenance information, that it is clear at which level the variables are associated, that I have any quality control/evaluation information, and that I have a narrative for what I did. This is meant to ensure that I maintain transparency and diligence while adding new variables. THis will help me to improve on continuous development, instead of running a big experiment and then tossing it in a folder. This could also include an interface for viewing and editing the db visually. 

For production protocols, this is more about serving the db for analysis, visualization, etc.. Here, I'm thinking about having some wiki-style visualization tools that allow for exploring the db, viewing the results of some existing analysis rounds, etc.. Essentially, all operations that don't involve editing the contents of the db. 



