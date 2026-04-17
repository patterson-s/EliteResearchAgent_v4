"""Pydantic response models for the Prosopography Explorer API."""

from typing import Optional
from pydantic import BaseModel


# ── HLP ──────────────────────────────────────────────────────────────────────

class HLPItem(BaseModel):
    hlp_id: int
    hlp_name: str
    hlp_year: int
    un_sg: Optional[str] = None
    member_count: int


# ── Persons ───────────────────────────────────────────────────────────────────

class PersonListItem(BaseModel):
    person_id: int
    display_name: str
    birth_year: Optional[int] = None
    death_status: Optional[str] = None
    hlp_id: int
    hlp_name: str
    nationalities: list[str] = []
    position_count: int


class PersonListResponse(BaseModel):
    total: int
    items: list[PersonListItem]


class PersonAttributeItem(BaseModel):
    attribute_name: str
    attribute_value: str
    attribute_label: Optional[str] = None
    confidence: Optional[str] = None
    extra_data: Optional[dict] = None


class PersonFilterMeta(BaseModel):
    hlp_panels: list[HLPItem]
    nationalities: list[str]
    birth_decades: list[int]
    career_domains: list[str] = []
    career_typologies: list[str] = []
    functional_mobility_types: list[str] = []


class PositionTagItem(BaseModel):
    domain: Optional[list[str]] = None
    organization_type: Optional[str] = None
    un_placement: Optional[str] = None
    geographic_scope: Optional[str] = None
    role_type: Optional[str] = None
    function: Optional[str] = None
    career_phase: Optional[str] = None
    policy_bridge: Optional[bool] = None


class CareerPositionItem(BaseModel):
    position_id: int
    title: str
    organization: Optional[str] = None
    org_id: Optional[int] = None
    org_canonical_name: Optional[str] = None
    time_start: Optional[int] = None
    time_finish: Optional[int] = None
    approximate_period: Optional[str] = None
    role_type: Optional[str] = None
    confidence: Optional[str] = None
    event_source: Optional[str] = None
    sort_order: int = 0
    tags: Optional[PositionTagItem] = None
    functional_tags: list[str] = []


class EducationItem(BaseModel):
    education_id: int
    degree_name: Optional[str] = None
    degree_type: Optional[str] = None
    field: Optional[str] = None
    institution: Optional[str] = None
    institution_country: Optional[str] = None
    time_start: Optional[int] = None
    time_finish: Optional[int] = None
    event_source: Optional[str] = None
    sort_order: int = 0


class AwardItem(BaseModel):
    award_id: int
    award_name: str
    awarding_organization: Optional[str] = None
    award_type: Optional[str] = None
    time_start: Optional[int] = None
    confidence: Optional[str] = None
    event_source: Optional[str] = None
    sort_order: int = 0


class PersonDetail(BaseModel):
    person_id: int
    display_name: str
    birth_year: Optional[int] = None
    death_status: Optional[str] = None
    death_year: Optional[int] = None
    hlp_id: int
    hlp_name: str
    hlp_year: int
    hlp_nomination_age: Optional[int] = None
    nationalities: list[str] = []
    attributes: list[PersonAttributeItem] = []
    career_positions: list[CareerPositionItem] = []
    education: list[EducationItem] = []
    awards: list[AwardItem] = []
    functional_tags: list[str] = []
    notes: Optional[str] = None


class PersonNotesRequest(BaseModel):
    note_text: str


# ── Organizations ─────────────────────────────────────────────────────────────

class OrgListItem(BaseModel):
    org_id: int
    canonical_name: str
    meta_type: Optional[str] = None
    sector: Optional[str] = None
    location_country: Optional[str] = None
    location_city: Optional[str] = None
    corpus_member_count: int = 0
    review_status: Optional[str] = None


class OrgListResponse(BaseModel):
    total: int
    items: list[OrgListItem]


class OrgFilterMeta(BaseModel):
    meta_types: list[str]
    sectors: list[str]


class OrgTooltip(BaseModel):
    org_id: int
    canonical_name: str
    meta_type: Optional[str] = None
    sector: Optional[str] = None
    location_country: Optional[str] = None
    location_city: Optional[str] = None
    un_hierarchical_tags: Optional[list[str]] = None
    gov_hierarchical_tags: Optional[list[str]] = None
    corpus_member_count: int = 0
    ontology_category: Optional[str] = None
    ontology_equivalence_class: Optional[str] = None
    ontology_hierarchy_path: Optional[list[str]] = None
    ontology_thematic_tags: Optional[list[str]] = None


# ── Functional Tags ────────────────────────────────────────────────────────────

class FunctionalTagsItem(BaseModel):
    entity_type: str
    entity_id: int
    tags: list[str] = []


class FunctionalTagsUpsertRequest(BaseModel):
    tags: list[str]


class FunctionalTagVocabItem(BaseModel):
    tag_name: str
    use_count: int


class OrgCorpusMemberRole(BaseModel):
    title: str
    time_start: Optional[int] = None
    time_finish: Optional[int] = None
    role_type: Optional[str] = None


class OrgCorpusMember(BaseModel):
    person_id: int
    display_name: str
    hlp_id: int
    hlp_name: str
    roles: list[OrgCorpusMemberRole] = []


class OrgDetail(BaseModel):
    org_id: int
    canonical_name: str
    meta_type: Optional[str] = None
    org_types: Optional[list[str]] = None
    sector: Optional[str] = None
    location_country: Optional[str] = None
    location_city: Optional[str] = None
    un_canonical_tag: Optional[str] = None
    un_hierarchical_tags: Optional[list[str]] = None
    gov_canonical_tag: Optional[str] = None
    gov_hierarchical_tags: Optional[list[str]] = None
    review_status: Optional[str] = None
    aliases: list[str] = []
    corpus_members: list[OrgCorpusMember] = []


# ── Org Summary ───────────────────────────────────────────────

class TopOrgRow(BaseModel):
    org_id: int
    canonical_name: str
    meta_type: Optional[str] = None
    sector: Optional[str] = None
    person_count: int
    position_count: int
    hlp_panels: list[int] = []


class TopOrgsResponse(BaseModel):
    items: list[TopOrgRow]
    total: int


class TypeRow(BaseModel):
    meta_type: str
    person_count: int
    org_count: int
    position_count: int


class TypeSummaryResponse(BaseModel):
    items: list[TypeRow]


class OntologyRow(BaseModel):
    equivalence_class: str
    hierarchy_path: list[str] = []
    parent_category: Optional[str] = None
    ontology_category: Optional[str] = None
    person_count: int
    org_count: int
    position_count: int


class OntologySummaryResponse(BaseModel):
    items: list[OntologyRow]
    note: str


class SectorRow(BaseModel):
    sector: str
    person_count: int
    org_count: int
    position_count: int


class SectorSummaryResponse(BaseModel):
    items: list[SectorRow]


# ── Locations ─────────────────────────────────────────────────────────────────

class LocationItem(BaseModel):
    city: Optional[str] = None
    country: str
    region: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    location_score: int
    person_count: int
    position_count: int

class LocationSummaryResponse(BaseModel):
    level: str
    total_locations: int
    total_score: int
    items: list[LocationItem]


# ── Ontology ──────────────────────────────────────────────────────────────────

class OntologyRun(BaseModel):
    run_id: int
    run_name: str
    narrative: Optional[str] = None
    evaluation_status: str
    n_processed: Optional[int] = None
    category: Optional[str] = None


class OntologyQueueItem(BaseModel):
    org_id: int
    canonical_name: str
    meta_type: Optional[str] = None
    gov_canonical_tag: Optional[str] = None
    gov_hierarchical_tags: Optional[list[str]] = None
    location_country: Optional[str] = None
    is_reviewed: bool = False
    # Populated when already reviewed
    mapping_id: Optional[int] = None
    equivalence_class: Optional[str] = None
    country_code: Optional[str] = None
    destination_country: Optional[str] = None
    destination_organization: Optional[str] = None
    superior: Optional[str] = None
    parent_category: Optional[str] = None
    hierarchy_path: Optional[list[str]] = None
    display_label: Optional[str] = None
    annotation_notes: Optional[str] = None
    region: Optional[str] = None
    thematic_tags: Optional[list[str]] = None
    parent_org: Optional[str] = None
    parent_org_id: Optional[int] = None
    parent_orgs: Optional[list[str]] = None
    alias_of_org_id: Optional[int] = None
    alias_canonical_name: Optional[str] = None


class OntologyQueueResponse(BaseModel):
    run_id: int
    category: str
    total: int
    reviewed: int
    remaining: int
    items: list[OntologyQueueItem]


class OntologyProgress(BaseModel):
    run_id: int
    category: str
    total: int
    reviewed: int
    remaining: int


class OntologyMappingCreate(BaseModel):
    org_id: int
    run_id: int
    equivalence_class: str
    country_code: Optional[str] = None
    destination_country: Optional[str] = None
    destination_organization: Optional[str] = None
    superior: Optional[str] = None
    parent_category: Optional[str] = None
    hierarchy_path: Optional[list[str]] = None
    display_label: Optional[str] = None
    annotation_notes: Optional[str] = None
    region: Optional[str] = None
    thematic_tags: Optional[list[str]] = None
    parent_org: Optional[str] = None
    parent_org_id: Optional[int] = None
    parent_orgs: Optional[list[str]] = None
    alias_of_org_id: Optional[int] = None
    new_class_label: Optional[str] = None  # if set, upserts a new user-defined class


class OntologyMappingResponse(BaseModel):
    mapping_id: int
    org_id: int
    run_id: int
    equivalence_class: str
    country_code: Optional[str] = None
    destination_country: Optional[str] = None
    destination_organization: Optional[str] = None
    superior: Optional[str] = None
    parent_category: Optional[str] = None
    hierarchy_path: Optional[list[str]] = None
    display_label: Optional[str] = None
    annotation_notes: Optional[str] = None
    region: Optional[str] = None
    thematic_tags: Optional[list[str]] = None
    parent_org: Optional[str] = None
    parent_org_id: Optional[int] = None
    parent_orgs: Optional[list[str]] = None
    alias_of_org_id: Optional[int] = None
    annotated_by: str


class OntologyEquivalenceClass(BaseModel):
    value: str
    label: str
    level: int  # 1=root, 2=branch, 3=sub-unit, 4+=user-defined
    parent_class: Optional[str] = None  # needed for JS hierarchy computation


class OntologyUserClass(BaseModel):
    value: str
    label: str
    parent_class: str
    category: str


class ParentOrgCandidate(BaseModel):
    org_id: int
    canonical_name: str
    match_method: str  # "exact_name" | "alias" | "stripped"


class ParentOrgResolutionItem(BaseModel):
    parent_org_text: str
    mapping_count: int
    suggestions: list[ParentOrgCandidate]


class ParentOrgResolutionQueue(BaseModel):
    run_id: int
    total_resolved: int
    total_unresolved: int
    items: list[ParentOrgResolutionItem]


class ParentOrgResolveRequest(BaseModel):
    run_id: int
    parent_org_text: str
    parent_org_id: int


class ParentOrgResolveResponse(BaseModel):
    updated_count: int
    parent_org_text: str
    parent_org_id: int
    org_canonical_name: str


class OntologyReviewItem(BaseModel):
    mapping_id: int
    org_id: int
    canonical_name: str
    display_label: Optional[str] = None
    equivalence_class: str
    parent_category: Optional[str] = None
    hierarchy_path: Optional[list[str]] = None
    parent_org: Optional[str] = None
    parent_org_id: Optional[int] = None
    parent_orgs: Optional[list[str]] = None
    parent_org_resolved: Optional[str] = None  # canonical_name of resolved parent org
    region: Optional[str] = None
    thematic_tags: Optional[list[str]] = None
    annotation_notes: Optional[str] = None
    review_status: str = 'pending'


class OntologyReviewResponse(BaseModel):
    run_id: int
    run_name: str
    evaluation_status: str
    total: int
    approved: int
    flagged: int
    pending: int
    items: list[OntologyReviewItem]


class OntologyMappingPatch(BaseModel):
    equivalence_class: Optional[str] = None
    parent_category: Optional[str] = None
    parent_org: Optional[str] = None
    parent_org_id: Optional[int] = None
    parent_orgs: Optional[list[str]] = None
    alias_of_org_id: Optional[int] = None
    display_label: Optional[str] = None
    region: Optional[str] = None
    thematic_tags: Optional[list[str]] = None
    annotation_notes: Optional[str] = None
    review_status: Optional[str] = None  # 'pending' | 'approved' | 'flagged'


class OrgSearchResult(BaseModel):
    org_id: int
    canonical_name: str
    un_canonical_tag: Optional[str] = None
    meta_type: Optional[str] = None


class OrgCreateRequest(BaseModel):
    canonical_name: str
    meta_type: str = "io"
    un_canonical_tag: Optional[str] = None


class OntologyRunFinalizeResponse(BaseModel):
    run_id: int
    evaluation_status: str
    pending_count: int  # 0 on success; >0 if blocked
    message: str


class OntologyClassSummaryItem(BaseModel):
    value: str
    label: str
    level: int
    parent_class: Optional[str] = None
    count: int = 0
    is_user_defined: bool = False


class OntologyClassRenameRequest(BaseModel):
    run_id: int
    old_value: str
    new_value: str
    new_label: str


class OntologyClassRenameResponse(BaseModel):
    old_value: str
    new_value: str
    updated_count: int


class OntologyOrgPosition(BaseModel):
    person_id: int
    display_name: str
    hlp_name: str
    title: str
    time_start: Optional[int] = None
    time_finish: Optional[int] = None


class OntologyOrgContext(BaseModel):
    org_id: int
    match_type: str  # "direct" | "approximate" | "sibling" | "none"
    positions: list[OntologyOrgPosition]


class OrgSplitSpec(BaseModel):
    new_canonical_name: str
    titles: list[str]  # exact title strings to reassign to this new org


class OrgSplitRequest(BaseModel):
    splits: list[OrgSplitSpec]


class OrgSplitNewOrg(BaseModel):
    org_id: int
    canonical_name: str
    position_count: int


class OrgSplitResult(BaseModel):
    original_org_id: int
    new_orgs: list[OrgSplitNewOrg]


# ── Search ────────────────────────────────────────────────────────────────────

class SearchResultItem(BaseModel):
    type: str          # "person" | "organization"
    id: int
    label: str
    sublabel: Optional[str] = None


class SearchResponse(BaseModel):
    query: str
    results: list[SearchResultItem]
