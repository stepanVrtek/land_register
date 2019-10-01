from land_register.pipelines import land_register_pipeline as lrp
from unittest.mock import MagicMock

pozemek_current_items = [{
    'ext_id_parcely': 200,
    'cislo_zaznamu': 1,
    'obec': 'A'
}]
pozemek_new_items = [{
    'ext_id_parcely': 200,
    'obec': 'A'
}]

test_lv_id = 200


# lrp.get_current_items = MagicMock(
#     return_value={}  # pozemek_current_items
# )


def test_item_change():
    lrp.filter_items_changes(test_lv_id, 'pozemek', pozemek_new_items)
    lrp.save_items('pozemek', pozemek_new_items, test_lv_id)


if __name__ == '__main__':
    test_item_change()
