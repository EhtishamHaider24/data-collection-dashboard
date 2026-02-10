# modules/collector.py
import os
from modules.db import execute_query
from modules.queries import *
from modules.utils import setup_logger

logger = setup_logger("COLLECTOR_LOGGER", os.path.join("logs", "collector.log"))

CATEGORY_IDS = {
    'Type_A': '89722275-ae54-2805-0349-60c52136888e',
    'Type_B': '07b38559-8bf8-cfaf-68ac-65dc0b77a7f4',
    'Type_A_and_B': 'e594a3ad-1d6d-150b-2bab-2287928e5450',
    'Type_C': '6fc336d2-3d1f-b9c2-d5f2-86ff03305af5'
}

REGIONS = ['Region_A', 'Region_B', 'Region_C']
COLLECTED_BY_TYPES = ['OBD', 'IVR', 'SMS']


# -------------------
# GENERAL STATS
# -------------------
def fetch_general_stats(collected_by):
    try:
        stats = {}
        stats['total_users'] = (execute_query(total_users_query(collected_by)) or [
                                {'total_users': 0}])[0].get('total_users', 0)
        stats['complete_profiles'] = (execute_query(complete_profiles_query(collected_by)) or [
                                      {'complete_profiles': 0}])[0].get('complete_profiles', 0)
        stats['semi_profiled_users'] = (execute_query(semi_profiled_users_query(
            collected_by)) or [{'semi_profiled_users': 0}])[0].get('semi_profiled_users', 0)

        cat_counts = [(execute_query(category_count_query(cid, collected_by)) or [
                       {'count': 0}])[0].get('count', 0) for cid in CATEGORY_IDS.values()]
        stats['category_marked_users'] = sum(cat_counts)
        stats['type_a_count'] = (execute_query(category_count_query(
            CATEGORY_IDS['Type_A'], collected_by)) or [{'count': 0}])[0].get('count', 0)
        stats['type_b_users'] = (execute_query(category_count_query(
            CATEGORY_IDS['Type_B'], collected_by)) or [{'count': 0}])[0].get('count', 0)
        stats['type_a_and_b'] = (execute_query(category_count_query(
            CATEGORY_IDS['Type_A_and_B'], collected_by)) or [{'count': 0}])[0].get('count', 0)
        stats['type_c'] = (execute_query(category_count_query(
            CATEGORY_IDS['Type_C'], collected_by)) or [{'count': 0}])[0].get('count', 0)

        stats['users_with_area'] = (execute_query(
            users_with_area_query(collected_by)) or [{'count': 0}])[0].get('count', 0)
        stats['item_selected_users'] = (execute_query(
            item_selected_users_query(collected_by)) or [{'count': 0}])[0].get('count', 0)
        stats['mobile_wallet_users'] = (execute_query(mobile_wallet_query(
            collected_by, yes=True)) or [{'count': 0}])[0].get('count', 0)
        stats['not_mobile_wallet_users'] = (execute_query(mobile_wallet_query(
            collected_by, yes=False)) or [{'count': 0}])[0].get('count', 0)
        stats['engaged_users'] = (execute_query(
            engaged_users_query(collected_by)) or [{'count': 0}])[0].get('count', 0)
        stats['charged_users'] = (execute_query(
            charged_users_query(collected_by)) or [{'count': 0}])[0].get('count', 0)

        logger.info(f"GENERAL STATS FETCHED FOR {collected_by}".upper())
        return stats
    except Exception as e:
        logger.error(f"ERROR FETCHING GENERAL STATS: {str(e).upper()}")
        raise


def store_general_stats(stats, collected_by):
    try:
        query = f"""
        INSERT INTO collection_main
        (collected_by, total_users, complete_profiles, semi_profiled_users, category_marked_users,
         type_a_count, type_b_users, type_a_and_b, type_c, users_with_area,
         item_selected_users, mobile_wallet_users, not_mobile_wallet_users, engaged_users,
         charged_users)
        VALUES
        ('{collected_by}', {stats.get('total_users', 0)}, {stats.get('complete_profiles', 0)}, {stats.get('semi_profiled_users', 0)},
         {stats.get('category_marked_users', 0)}, {stats.get('type_a_count', 0)}, {stats.get('type_b_users', 0)},
         {stats.get('type_a_and_b', 0)}, {stats.get('type_c', 0)}, {stats.get('users_with_area', 0)},
         {stats.get('item_selected_users', 0)}, {stats.get('mobile_wallet_users', 0)}, {stats.get('not_mobile_wallet_users', 0)},
         {stats.get('engaged_users', 0)}, {stats.get('charged_users', 0)}
        );
        """
        execute_query(query, fetch=False, reporting=True)
        logger.info(f"GENERAL STATS STORED FOR {collected_by}".upper())
    except Exception as e:
        logger.error(f"ERROR STORING GENERAL STATS: {str(e).upper()}")
        raise


# -------------------
# REGION & ITEM STATS
# -------------------
def fetch_region_stats(region, collected_by):
    try:
        stats = {}
        # Category / counts
        stats['category_marked'] = (execute_query(
            f"SELECT COUNT(*) AS count FROM users u JOIN locations d ON u.location_id=d.id JOIN locations r ON d.parent_id=r.id WHERE r.name='{region}' AND u.collected_by='{collected_by}' AND u.category_id IS NOT NULL"
        ) or [{'count': 0}])[0].get('count', 0)

        stats['type_a'] = (execute_query(category_count_query(
            CATEGORY_IDS['Type_A'], collected_by, region)) or [{'count': 0}])[0].get('count', 0)
        stats['type_b'] = (execute_query(category_count_query(
            CATEGORY_IDS['Type_B'], collected_by, region)) or [{'count': 0}])[0].get('count', 0)
        stats['type_a_and_b'] = (execute_query(category_count_query(
            CATEGORY_IDS['Type_A_and_B'], collected_by, region)) or [{'count': 0}])[0].get('count', 0)
        stats['type_c'] = (execute_query(category_count_query(
            CATEGORY_IDS['Type_C'], collected_by, region)) or [{'count': 0}])[0].get('count', 0)

        # Area metrics
        stats['area_selected'] = (execute_query(users_with_area_query(
            collected_by, region)) or [{'count': 0}])[0].get('count', 0)
        stats['area_0_to_7'] = (execute_query(
            area_range_query(collected_by, region, 1)
        ) or [{'count': 0}])[0].get('count', 0)
        stats['area_7_to_12'] = (execute_query(
            area_range_query(collected_by, region, 7)
        ) or [{'count': 0}])[0].get('count', 0)
        stats['area_12_plus'] = (execute_query(
            area_range_query(collected_by, region, 12)
        ) or [{'count': 0}])[0].get('count', 0)

        # Mobile wallet
        stats['mobile_wallet_yes'] = (execute_query(mobile_wallet_query(
            collected_by, yes=True, region=region)) or [{'count': 0}])[0].get('count', 0)
        stats['mobile_wallet_no'] = (execute_query(mobile_wallet_query(
            collected_by, yes=False, region=region)) or [{'count': 0}])[0].get('count', 0)

        # Item lists
        stats['items_type_1'] = execute_query(
            item_type_query(collected_by, region, 'type_1')) or []
        stats['items_type_2'] = execute_query(
            item_type_query(collected_by, region, 'type_2')) or []
        stats['items_type_3'] = execute_query(
            item_type_query(collected_by, region, 'type_3')) or []

        logger.info(
            f"REGION STATS FETCHED: {region} | COLLECTED_BY: {collected_by}".upper())
        return stats
    except Exception as e:
        logger.error(
            f"ERROR FETCHING REGION STATS: {region} | {collected_by} | {str(e).upper()}")
        raise


def store_region_stats(region, stats, collected_by):
    try:
        insert_region_q = f"""
        INSERT INTO collection_region
        (collected_by, region, category_marked, type_a, type_b, type_a_and_b,
         type_c, area_selected, area_0_to_7, area_7_to_12, area_12_plus,
         mobile_wallet_yes, mobile_wallet_no)
        VALUES
        ('{collected_by}', '{region}',
         {stats.get('category_marked', 0)},
         {stats.get('type_a', 0)},
         {stats.get('type_b', 0)},
         {stats.get('type_a_and_b', 0)},
         {stats.get('type_c', 0)},
         {stats.get('area_selected', 0)},
         {stats.get('area_0_to_7', 0)},
         {stats.get('area_7_to_12', 0)},
         {stats.get('area_12_plus', 0)},
         {stats.get('mobile_wallet_yes', 0)},
         {stats.get('mobile_wallet_no', 0)}
        );
        """
        execute_query(insert_region_q, fetch=False, reporting=True)
        logger.info(
            f"REGION ROW INSERTED: {region} | COLLECTED_BY: {collected_by}".upper())

        # Insert items safely
        for item_list, itype in [(stats.get('items_type_1', []), 'type_1'),
                                 (stats.get('items_type_2', []), 'type_2'),
                                 (stats.get('items_type_3', []), 'type_3')]:
            for item in item_list:
                try:
                    title = (item.get('title') or item.get(
                        'item_title') or '').replace("'", "''")
                    count = item.get('user_count') or item.get('count') or 0
                    q = f"""
                    INSERT INTO collection_region_item
                    (collected_by, region, item_type, item_title, user_count)
                    VALUES
                    ('{collected_by}', '{region}', '{itype}', '{title}', {count});
                    """
                    execute_query(q, fetch=False, reporting=True)
                    logger.info(
                        f"{itype.upper()} INSERTED: {title} | REGION: {region} | COLLECTED_BY: {collected_by}".upper())
                except Exception as e_item:
                    logger.error(
                        f"ERROR INSERTING {itype.upper()} ROW: {str(e_item).upper()} | ITEM: {str(item)}")

        logger.info(
            f"REGION STATS STORED: {region} | COLLECTED_BY: {collected_by}".upper())
    except Exception as e:
        logger.error(
            f"ERROR STORING REGION STATS: {region} | {collected_by} | {str(e).upper()}")
        raise


# -------------------
# RUN HELPERS
# -------------------
def run_all_general(collected_by_list=COLLECTED_BY_TYPES):
    for collected_by in collected_by_list:
        logger.info(f"PROCESSING GENERAL DATA FOR {collected_by}".upper())
        stats = fetch_general_stats(collected_by)
        store_general_stats(stats, collected_by)
        logger.info(f"COMPLETED GENERAL DATA FOR {collected_by}".upper())


def run_region_loop(collected_by_list=COLLECTED_BY_TYPES):
    for collected_by in collected_by_list:
        logger.info(f"STARTING REGION LOOP FOR: {collected_by}".upper())
        for region in REGIONS:
            logger.info(
                f"PROCESSING REGION: {region} | COLLECTED_BY: {collected_by}".upper())
            stats = fetch_region_stats(region, collected_by)
            store_region_stats(region, stats, collected_by)
        logger.info(f"COMPLETED REGION LOOP FOR: {collected_by}".upper())
