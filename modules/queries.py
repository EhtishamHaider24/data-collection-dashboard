# =========================
# COLLECTION QUERIES
# =========================

def total_users_query(collected_by, region=None):
    query = f"SELECT COUNT(id) AS total_users FROM users WHERE collected_by = '{collected_by}'"
    if region:
        query += f" AND location_id IN (SELECT id FROM locations WHERE parent_id IN " \
            f"(SELECT id FROM locations WHERE type='region' AND name='{region}'))"
    return query


def complete_profiles_query(collected_by, region=None):
    query = f"""
    SELECT COUNT(DISTINCT u.id) AS complete_profiles
    FROM users u
    LEFT JOIN properties p ON u.id = p.user_id
    LEFT JOIN property_items pi ON p.id = pi.property_id
    LEFT JOIN asset_properties ap ON u.id = ap.user_id
    LEFT JOIN asset_property_details apd ON ap.id = apd.asset_property_id
    LEFT JOIN categories c ON c.id = u.category_id
    WHERE u.collected_by = '{collected_by}'
    """
    if region:
        query += f" AND u.location_id IN (SELECT id FROM locations WHERE parent_id IN " \
            f"(SELECT id FROM locations WHERE type='region' AND name='{region}'))"
    query += " AND (CASE " \
             "WHEN c.title = 'Type C' THEN TRUE " \
             "WHEN c.title IN ('Interest Type 1','Interest Type 2','Interest Type 3','Interest Type 4') " \
             "THEN u.location_id IS NOT NULL AND u.lat IS NOT NULL AND u.lng IS NOT NULL AND u.wallet_consent IS NOT NULL AND p.id IS NULL AND ap.id IS NULL " \
             "WHEN c.title = 'Only Type B' THEN u.location_id IS NOT NULL AND u.lat IS NOT NULL AND u.lng IS NOT NULL AND u.wallet_consent IS NOT NULL AND ap.id IS NOT NULL AND apd.asset_id IS NOT NULL AND apd.count IS NOT NULL " \
             "WHEN c.title = 'Type A' THEN u.location_id IS NOT NULL AND u.lat IS NOT NULL AND u.lng IS NOT NULL AND u.wallet_consent IS NOT NULL AND p.id IS NOT NULL AND pi.item_id IS NOT NULL " \
             "WHEN c.title = 'Type A and Type B' THEN u.location_id IS NOT NULL AND u.lat IS NOT NULL AND u.lng IS NOT NULL AND u.wallet_consent IS NOT NULL AND p.id IS NOT NULL AND pi.item_id IS NOT NULL AND ap.id IS NOT NULL AND apd.asset_id IS NOT NULL AND apd.count IS NOT NULL " \
             "ELSE FALSE END)"
    return query


def semi_profiled_users_query(collected_by, region=None):
    query = f"""
    SELECT COUNT(DISTINCT u.id) AS semi_profiled_users
    FROM users u
    LEFT JOIN properties p ON u.id = p.user_id
    LEFT JOIN property_items pi ON p.id = pi.property_id
    LEFT JOIN asset_properties ap ON u.id = ap.user_id
    LEFT JOIN asset_property_details apd ON ap.id = apd.asset_property_id
    LEFT JOIN categories c ON c.id = u.category_id
    WHERE u.collected_by = '{collected_by}'
    AND NOT (CASE 
        WHEN c.title = 'Type C' THEN TRUE
        WHEN c.title IN ('Interest Type 1','Interest Type 2','Interest Type 3','Interest Type 4') 
            THEN u.location_id IS NOT NULL AND u.lat IS NOT NULL AND u.lng IS NOT NULL AND u.wallet_consent IS NOT NULL AND p.id IS NULL AND ap.id IS NULL
        WHEN c.title = 'Only Type B' THEN u.location_id IS NOT NULL AND u.lat IS NOT NULL AND u.lng IS NOT NULL AND u.wallet_consent IS NOT NULL AND ap.id IS NOT NULL AND apd.asset_id IS NOT NULL AND apd.count IS NOT NULL
        WHEN c.title = 'Type A' THEN u.location_id IS NOT NULL AND u.lat IS NOT NULL AND u.lng IS NOT NULL AND u.wallet_consent IS NOT NULL AND p.id IS NOT NULL AND pi.item_id IS NOT NULL
        WHEN c.title = 'Type A and Type B' THEN u.location_id IS NOT NULL AND u.lat IS NOT NULL AND u.lng IS NOT NULL AND u.wallet_consent IS NOT NULL AND p.id IS NOT NULL AND pi.item_id IS NOT NULL AND ap.id IS NOT NULL AND apd.asset_id IS NOT NULL AND apd.count IS NOT NULL
        ELSE FALSE
    END)
    AND (
        u.location_id IS NOT NULL OR u.lat IS NOT NULL OR u.lng IS NOT NULL OR u.wallet_consent IS NOT NULL OR p.id IS NOT NULL OR pi.item_id IS NOT NULL OR ap.id IS NOT NULL OR apd.asset_id IS NOT NULL
    )
    """
    if region:
        query += f" AND u.location_id IN (SELECT id FROM locations WHERE parent_id IN " \
            f"(SELECT id FROM locations WHERE type='region' AND name='{region}'))"
    return query


def category_count_query(category_id, collected_by, region=None):
    query = f"SELECT COUNT(id) AS count FROM users WHERE category_id = '{category_id}' AND collected_by = '{collected_by}'"
    if region:
        query += f" AND location_id IN (SELECT id FROM locations WHERE parent_id IN " \
            f"(SELECT id FROM locations WHERE type='region' AND name='{region}'))"
    return query


def users_with_area_query(collected_by, region=None):
    query = f"SELECT COUNT(DISTINCT user_id) AS count FROM properties WHERE user_id IN " \
        f"(SELECT id FROM users WHERE collected_by = '{collected_by}') AND area IS NOT NULL"
    if region:
        query += f" AND user_id IN (SELECT id FROM users WHERE location_id IN (SELECT id FROM locations WHERE parent_id IN " \
            f"(SELECT id FROM locations WHERE type='region' AND name='{region}')))"
    return query


def item_selected_users_query(collected_by, region=None):
    query = f"""
    SELECT COUNT(DISTINCT p.user_id) AS count
    FROM properties p
    INNER JOIN property_items pi ON p.id = pi.property_id
    WHERE p.user_id IN (SELECT id FROM users WHERE collected_by = '{collected_by}')
      AND pi.item_id IS NOT NULL
    """
    if region:
        query += f" AND p.user_id IN (SELECT id FROM users WHERE location_id IN (SELECT id FROM locations WHERE parent_id IN " \
            f"(SELECT id FROM locations WHERE type='region' AND name='{region}')))"
    return query


def mobile_wallet_query(collected_by, yes=True, region=None):
    val = '1' if yes else '0'
    query = f"SELECT COUNT(id) AS count FROM users WHERE collected_by = '{collected_by}' AND is_wallet_user = '{val}'"
    if region:
        query += f" AND location_id IN (SELECT id FROM locations WHERE parent_id IN " \
            f"(SELECT id FROM locations WHERE type='region' AND name='{region}'))"
    return query


def engaged_users_query(collected_by):
    query = f"""
    SELECT COUNT(DISTINCT u.id) AS count
    FROM users u
    INNER JOIN mau_dau.active_users a ON a.msisdn = u.id
    WHERE a.paidwall = 'service_1'
      AND a.activity_date >= NOW() - INTERVAL 30 DAY
      AND u.collected_by = '{collected_by}'
    """
    return query


def charged_users_query(collected_by):
    query = f"""
    SELECT COUNT(DISTINCT u.id) AS count
    FROM users u
    INNER JOIN charge_history.service_1_90_day_charged a ON a.cellno = u.id
    WHERE a.created >= NOW() - INTERVAL 30 DAY
      AND u.collected_by = '{collected_by}'
    """
    return query


# ---------- REGION ITEM / TYPE QUERIES ---------- #
def area_range_query(collected_by, region, area_value):
    query = f"""
        SELECT COUNT(*) AS count
        FROM properties p
        JOIN users u ON p.user_id = u.id
        WHERE u.collected_by = '{collected_by}'
          AND p.area = {area_value}
    """
    if region:
        query += f" AND u.location_id IN (SELECT id FROM locations WHERE parent_id IN " \
            f"(SELECT id FROM locations WHERE type='region' AND name='{region}'))"
    return query


def total_items_query(collected_by, region=None):
    query = f"""
    SELECT COUNT(DISTINCT p.user_id) AS count
    FROM properties p
    JOIN property_items pi ON p.id = pi.property_id
    WHERE p.user_id IN (SELECT id FROM users WHERE collected_by = '{collected_by}')
      AND pi.item_id IS NOT NULL
    """
    if region:
        query += f" AND p.user_id IN (SELECT id FROM users WHERE location_id IN (SELECT id FROM locations WHERE parent_id IN " \
            f"(SELECT id FROM locations WHERE type='region' AND name='{region}')))"
    return query


def item_type_query(collected_by, region=None, item_type='type_1'):
    query = f"""
    SELECT i.title, COUNT(DISTINCT p.user_id) AS user_count
    FROM properties p
    JOIN property_items pi ON p.id = pi.property_id
    JOIN items i ON pi.item_id = i.id
    WHERE p.user_id IN (SELECT id FROM users WHERE collected_by = '{collected_by}')
      AND i.item_type = '{item_type}'
    """
    if region:
        query += f" AND p.user_id IN (SELECT id FROM users WHERE location_id IN (SELECT id FROM locations WHERE parent_id IN " \
            f"(SELECT id FROM locations WHERE type='region' AND name='{region}')))"
    query += " GROUP BY i.title ORDER BY i.title"
    return query
