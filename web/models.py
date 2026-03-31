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


class PersonFilterMeta(BaseModel):
    hlp_panels: list[HLPItem]
    nationalities: list[str]
    birth_decades: list[int]
    career_domains: list[str] = []
    career_typologies: list[str] = []


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


# ── Search ────────────────────────────────────────────────────────────────────

class SearchResultItem(BaseModel):
    type: str          # "person" | "organization"
    id: int
    label: str
    sublabel: Optional[str] = None


class SearchResponse(BaseModel):
    query: str
    results: list[SearchResultItem]
