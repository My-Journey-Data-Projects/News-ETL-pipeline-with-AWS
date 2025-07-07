from dataclasses import dataclass, field

@dataclass
class S3ConnectionParams():
    access_id: str
    secret_access_key: str
    region_name: str

@dataclass
class ResultModel():
    article_id: str
    title: str
    link: str
    keywords: list[str]
    creator: list[str]
    description: str
    content: str
    pubDate: str
    pubDateTZ: str
    image_url: str
    video_url: str
    source_id: str
    source_name: str
    source_priority: int
    source_url: str
    source_icon: str
    language: str
    country: list[str]
    category: list[str]
    duplicate: bool

@dataclass
class ResponseModel():
    status: str
    totalResults: int = 0
    results: ResultModel = None
    nextPage: str = ""

@dataclass
class ResultList():
    data: list[ResultModel] = field(default_factory=list)





