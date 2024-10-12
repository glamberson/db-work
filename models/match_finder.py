# models/match_finder.py

from models.data_models import StagedRecord, MRLRecord, Match
from models.database import DatabaseConnection
from Levenshtein import ratio
import logging
from utils.logging_config import clean_currency_string

logger = logging.getLogger(__name__)

class MatchFinder:
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection

    def find_potential_matches(self, search_records):
        potential_matches = []
        for search_record in search_records:
            logger.debug(f"Searching for matches for record: {search_record.twcode}")

            # First, try to match by twcode
            query = """
            SELECT * FROM mrl_line_items
            WHERE twcode = %s
            AND NOT EXISTS (
                SELECT 1 FROM staged_egypt_weekly_data
                WHERE jcn = mrl_line_items.jcn
                AND twcode = mrl_line_items.twcode
                AND (mrl_matched = TRUE OR fulfillment_matched = TRUE)
            )
            """
            params = (search_record.twcode,)
            results = self.db.execute_query(query, params)

            if not results:
                logger.debug(f"No exact TWCODE match found for {search_record.twcode}")
                # If no match by TWCODE, try other fields
                query = """
                SELECT * FROM mrl_line_items
                WHERE jcn = %s OR niin = %s OR part_no = %s
                """
                params = (search_record.jcn, search_record.niin, search_record.part_no)
                results = self.db.execute_query(query, params)

            logger.debug(f"Found {len(results)} potential matches for search record: {search_record.twcode}")

            for result in results:
                mrl_record = self._dict_to_mrl_record(result)
                score, field_scores = self.calculate_match_score(search_record, mrl_record)
                potential_matches.append(Match(
                    search_record=search_record,
                    mrl_record=mrl_record,
                    score=score,
                    field_scores=field_scores
                ))

        # Sort potential matches by score in descending order
        potential_matches.sort(key=lambda match: match.score, reverse=True)

        return potential_matches

    def calculate_match_score(self, search_record: StagedRecord, mrl_record: MRLRecord):
        weights = {
            'twcode': 40,
            'jcn': 20,
            'nomenclature': 20,
            'niin': 10,
            'part_no': 10
        }

        field_scores = {}
        total_score = 0
        max_score = sum(weights.values())

        for field, weight in weights.items():
            search_value = getattr(search_record, field, '').lower().strip()
            mrl_value = getattr(mrl_record, field, '').lower().strip()

            logger.debug(f"Comparing {field}: search='{search_value}', mrl='{mrl_value}'")

            if field == 'nomenclature':
                similarity = ratio(search_value, mrl_value)
                field_scores[field] = similarity * 100
                total_score += similarity * weight
            elif search_value and mrl_value and search_value == mrl_value:
                field_scores[field] = 100
                total_score += weight
            else:
                field_scores[field] = 0

        normalized_score = (total_score / max_score) * 100
        return round(normalized_score, 2), field_scores

    def _dict_to_mrl_record(self, record_dict):
        # Lowercase keys to match dataclass field names
        record_dict = {k.lower(): v for k, v in record_dict.items()}

        # Exclude metadata fields
        metadata_fields = {'created_by', 'created_at', 'updated_by', 'updated_at', 'update_source', 'status_id'}
        record_data = {k: v for k, v in record_dict.items() if k not in metadata_fields}

        # Convert MONEY fields to float
        if 'market_research_up' in record_data and record_data['market_research_up'] is not None:
            record_data['market_research_up'] = float(clean_currency_string(record_data['market_research_up']))
        if 'market_research_ep' in record_data and record_data['market_research_ep'] is not None:
            record_data['market_research_ep'] = float(clean_currency_string(record_data['market_research_ep']))

        return MRLRecord(**record_data)
