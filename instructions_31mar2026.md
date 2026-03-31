I am working on the organizational ontology for the prosopography project. I want to work on standardizing the representation of some of the organizations. This will primarily be a manual editing effort, but I would like to have some kind of tool that will help to facilitate. 

I want to start by looking at how national governments are represented. To start, I'm looking at Amre Moussa's career events. 

Amre's career has primarily been in the Egyptian ministry of foreign affairs; however, he also worked in some party politics, constitutional assembly, etc, for the national government. 

Here are a few examples of Career events for Amre and how I want them to be stored

Organization: 
Government of (Egypt)
- Ministry of (Foreign Affairs)
	- Embassy in (Switzerland)
	- Mission to (United Nations)

Role: Diplomat, Mission Staff

Organization: 
Government of (Egypt)
- Ministry of (Foreign Affairs)

Role: Adviser to the Minister of Foreign Affairs 

There are others that are not associated with the Ministry of Foreign Affairs. However, I want to focus on this right now. 

Additionally, I want to develop this ontology in a way that can accommodate formats from other countries, such that I could know that the US State Department and the Egyptian MFA are the same type of organization. Perhaps there would be something about an org link that would map US State Department to Ministry of Foreign Affairs (USA)?

I'd like to consider this as a derivative - so, we are not going to overwrite the existing data. Rather, we are going to create a new mapping between the existing data and the new ontology. This ontology may at some point supersede the existing one, but not yet. 

I'm not sure how we can best implement this. I want to edit all of the organizations that would be considered Ministries of Foreign Affairs. I want to go through all of them and make this kind of mapping. Perhaps a kind of interface? Because the organizations are still unstandardized, you will have to come up with a way to provide me with organizations that are likely MFAs. 

Once I have come up with the template, it would be good if I could have some autocomplete to speed things up with the annotations and correcting. 

We are starting with MFAs, but we will proceed to other organization types downstream. It would be useful if we could have some kind of counter that shows our progress in manually going through all of the organizations. 