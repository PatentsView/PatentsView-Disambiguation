#
# import configparser
# from tqdm import tqdm
# from pv.disambiguation.core import load_inventor_mentions
# from pv.disambiguation.inventor.model import InventorModel
# import torch
# from grinch.agglom import Agglom
#
#
# def demo(rawinventor_tsv):
#     canopy = 'fl:m_ln:ramaseshan'
#     print('working on canopy %s ' % canopy)
#
#     config = configparser.ConfigParser()
#     config.read(['config/database_config.ini', 'config/inventor/run_clustering.ini', 'config/database_tables.ini'])
#
#     print('loading data... %s')
#     mentions = []
#     for im in tqdm(load_inventor_mentions(rawinventor_tsv)):
#         if im.canopy() == canopy:
#             mentions.append(im)
#
#     encoding_model = InventorModel.from_config(config)
#     features = encoding_model.encode(mentions)
#     model = torch.load(config['inventor']['model']).eval()
#     grinch = Agglom(model, features, num_points=len(mentions))
#     grinch.build_dendrogram_hac()
#     fc = grinch.flat_clustering(model.aux['threshold'])
#
#
#
