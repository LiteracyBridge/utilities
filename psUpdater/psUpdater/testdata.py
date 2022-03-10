import json
from argparse import Namespace
from datetime import date, datetime
from json import JSONEncoder
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

import db
import XlsExporter
import Spec
import ImportProcessor
import SpecCompare
import SpecHandler

_engine: Optional[Engine] = None
args: Optional[Namespace] = None

JSON_STR = '''
[{"deploymentnumber":1,"startdate":"2019-03-01","enddate":"2019-05-30","deployment":"TEST-19-1","playlists":[{"position":1,"title":"Health","audience":"","messages":[{"position":1,"title":"Breast milk is the best food for babies.","format":"","default_category_code":"Health","variant":"HP","sdg_goal_id":"4","sdg_target":"1","sdg_target_id":"4.1","key_points":"Breast milk is the best food for your baby.","languages":"","audience":""},{"position":2,"title":"Family Planning: song","format":"","default_category_code":"Health","variant":"~HP","sdg_goal_id":"5","sdg_target":"3","sdg_target_id":"5.3","key_points":"Family planning is good for you, for your family, and for your community.","languages":"en","audience":""},{"position":3,"title":"Neo-natal care: Immunizations, Breastfeeding, Handwashing","format":"","default_category_code":"Health","variant":"","sdg_goal_id":"6","sdg_target":null,"sdg_target_id":null,"key_points":"Take good care of your infant.","languages":"en","audience":""},{"position":4,"title":"ANC benefit: song","format":"","default_category_code":"Health","variant":"","sdg_goal_id":"1","sdg_target":"1","sdg_target_id":"1.1","key_points":"It's good to take good care of your infant.","languages":"","audience":""},{"position":5,"title":"Mansugo can cause serious health problems","format":"","default_category_code":"Health","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Mansugo is bad stuff.","languages":"","audience":""},{"position":6,"title":"Breastfeeding your newborn.","format":"","default_category_code":"Health","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"How to breastfeed.","languages":"","audience":""}]},{"position":2,"title":"Agriculture","audience":"","messages":[{"position":1,"title":"No-mounds: Plant your crops in rows instead of mounds.","format":"","default_category_code":"","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Increase yields and reduce erosion.","languages":"","audience":""},{"position":2,"title":"Test your seeds before sowing, and use the proper number per hole.","format":"","default_category_code":"","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Test seeds so you know if they're good.","languages":"","audience":""},{"position":3,"title":"Timing is important in planting and fertilizing","format":"","default_category_code":"","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"For every thing there is a season.","languages":"","audience":""}]},{"position":3,"title":"Livestock","audience":"","messages":[{"position":1,"title":"Vacinate livestock during the dry season, and house them to save their manure.","format":"","default_category_code":"Livestock","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"And a time for every thing.","languages":"","audience":""}]}]},{"deploymentnumber":2,"startdate":"2019-06-01","enddate":"2019-12-31","deployment":"TEST-19-2","playlists":[{"position":1,"title":"Intro Message","audience":"","messages":[{"position":1,"title":"What's new in Deployment 2","format":"","default_category_code":"","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"What to expect","languages":"","audience":""}]},{"position":2,"title":"Agriculture","audience":"","messages":[{"position":1,"title":"No-mounds: Plant your crops in rows instead of mounds.","format":"","default_category_code":"","variant":"~sc","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Increase yields and reduce erosion.","languages":"en","audience":""},{"position":2,"title":"Test your seeds before sowing, and use the proper number per hole.","format":"","default_category_code":"","variant":"~sc","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Test seeds so you know if they're good.","languages":"en","audience":""},{"position":3,"title":"Timing is important in planting and fertilizing","format":"","default_category_code":"","variant":"~sc","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"For every thing there is a season.","languages":"en","audience":""}]},{"position":3,"title":"Health","audience":"","messages":[{"position":1,"title":"Breast milk is the best food for babies.","format":"","default_category_code":"Health","variant":"~sc","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Breast milk is the best food for your baby.","languages":"en","audience":""},{"position":2,"title":"Family Planning: song","format":"","default_category_code":"Health","variant":"~sc","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Family planning is good for you, for your family, and for your community.","languages":"en","audience":""},{"position":3,"title":"Neo-natal care: Immunizations, Breastfeeding, Handwashing","format":"","default_category_code":"Health","variant":"~sc","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Take good care of your infant.","languages":"en","audience":""},{"position":4,"title":"ANC benefit: song","format":"","default_category_code":"Health","variant":"~sc","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"It's good to take good care of your infant.","languages":"en","audience":""},{"position":5,"title":"Mansugo can cause serious health problems","format":"","default_category_code":"Health","variant":"~sc","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Mansugo is bad stuff.","languages":"en","audience":""},{"position":6,"title":"Breastfeeding your newborn.","format":"","default_category_code":"Health","variant":"~sc","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"How to breastfeed.","languages":"en","audience":""}]},{"position":4,"title":"Livestock","audience":"","messages":[{"position":1,"title":"Vacinate livestock during the dry season, and house them to save their manure.","format":"","default_category_code":"Livestock","variant":"~sc","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"And a time for every thing.","languages":"en","audience":""}]},{"position":5,"title":"Sound Check","audience":"","messages":[{"position":1,"title":"Pink noise","format":"","default_category_code":"Music","variant":"sc","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Random sound that seems to be as loud at every frequency.","languages":"","audience":""},{"position":2,"title":"a220Hz","format":"","default_category_code":"Music","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"A low A.","languages":"","audience":""},{"position":3,"title":"a440Hz","format":"","default_category_code":"Music","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"A middle A.","languages":"","audience":""},{"position":4,"title":"1kHz","format":"","default_category_code":"Music","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"One thousand hertz.","languages":"","audience":""},{"position":5,"title":"2kHz","format":"","default_category_code":"Music","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Two thousand hertz.","languages":"","audience":""},{"position":6,"title":"4kHz","format":"","default_category_code":"Music","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Four thousand hertz.","languages":"","audience":""},{"position":7,"title":"Silence","format":"","default_category_code":"Music","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"And in the naked light I saw\\nTen thousand people, maybe more\\nPeople talking without speaking\\nPeople hearing without listening\\nPeople writing songs that voices never share\\nNo one dared\\nDisturb the sound of silence","languages":"","audience":""},{"position":8,"title":"Hip Hop","format":"Song","default_category_code":"Music","variant":"","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Pounding bass. Well, actually The High Violets.","languages":"","audience":""}]}]},{"deploymentnumber":3,"startdate":"2021-01-01","enddate":"2021-12-31","deployment":"TEST-21-3","playlists":[{"position":1,"title":"Antenatal Clinics","audience":"","messages":[{"position":1,"title":"Delivery with a skilled attendant is good for you, the baby and the community","format":"Drama","default_category_code":"Health","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Skilled attendants are accredited health professionals such as midwives, doctors, or nurses who have been educated and trained to proficiency in the skills needed to manage normal pregnancies, childbirth and the immediate postnatal period.\\n\\nChildbirth with a skilled attendant reduces the risk that the mother and/or the baby will die or experience complications from childbirth.\\n\\nDelivery at home might be comfortable, but delivery at a facility with a skilled attendant is the best way to ensure that you and your baby are healthy and will remain healthy.","languages":"","audience":""},{"position":2,"title":"Birth preparedness and planning","format":"Interview","default_category_code":"Health","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Begin planning how you will deliver with a skilled attendant as soon as you know you are pregnant. And register under the NHIS.","languages":"","audience":""},{"position":3,"title":"Pregnant women should deliver with a skilled attendant at a healthcare","format":"Song","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Remember…\\n• Always deliver at a health facility with a skilled birth attendant.\\n• A women should go for ANC at least 4 times during her pregnancy.","languages":"","audience":""},{"position":4,"title":"Detecting labour signs and delivery with a skilled birth attendant","format":"Drama","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"If you notice any of the following signs; visit the hospital immediately:\\nSudden uncontrolled flow of water from the vagina, so called Breaking of waters\\nRegular and strong contraction\\nYou notice a discharge\\nYou feel more cramps and increased back pain\\nYou experience any bleeding or bright-red discharge (not brown or pinkish).\\nYou experience blurred or double vision, a severe headache, or sudden swelling.\\n\\nThese can be symptoms of preeclampsia, which is characterized by pregnancy-induced high blood pressure and requires medical attention. \\n\\nOther preparations include planning ahead, having health insurance coverage and attending antenatal care clinic early and regularly should be included","languages":"","audience":""}]},{"position":2,"title":"Child Welfare Clinics","audience":"","messages":[{"position":1,"title":"Exclusive breastfeeding for six months","format":"Song","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"• Give your baby ONLY breast milk for the first six months of his or her life; this means no water, other liquid or foods, not even a little.","languages":"","audience":""},{"position":2,"title":"The healthiest babies are ones who are exclusively breastfed","format":"Drama","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Remember…\\n• Give your baby ONLY breast milk for the first six months of his or her life; this means no water, no other liquid or foods, not even a little.\\n• Breast milk alone is the best food for baby under six months old","languages":"","audience":""},{"position":3,"title":"Reducing chances of getting malaria","format":"Song","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Sleep under an LLIN each night to reduce your chances of getting malaria. Weed around your house and take away stagnant water, they produces mosquitoes. Remember, mosquitoe bites gives us malaria.","languages":"","audience":""},{"position":4,"title":"Malaria and how it is transmitted","format":"Interview","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Malaria is an illness caused by the presence of malarial parasite in the human body. It spreads through the bite of mosquito. When a mosquito bites a person suffering from malaria, the parasite enters the body of the mosquito. When the infected mosquito bites a healthy person then he/she may develop malaria.","languages":"","audience":""},{"position":5,"title":"Using ORS and Zinc tablets to treat diarrhea in children","format":"Song","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"1. Diarrhea is a leading killer of children. It kills children by draining liquid from the body, dehydrating the child.\\n\\n2. ORS is an effective way to replenish the fluids lost when a child has diarrhea and zinc tablets can reduce the duration and severity of a diarrhea bout. \\n\\n3. Using ORS and zinc in combination can save a child’s life.","languages":"","audience":""},{"position":6,"title":"Signs of diarrhoea and using ORS and Zinc tablets as treatment options","format":"Interview","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Remember…\\n• It is diarrhoea if your child or baby passes more than three watery stools in a day. If you do not treat this, your baby can die from diarrhoea.\\n• ORS is a very effective way to treat diarrhoea in your children. \\n• Give your child zinc tablets for 10 days, even if the diarrhoea has stopped, to prevent diarrhoea for 3 months.","languages":"","audience":""},{"position":7,"title":"Cause, effects and treatment of diarrhoea","format":"Song","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"1. Diarrhea is a leading killer of children. It kills children by draining liquid from the body, dehydrating the child.\\n\\n2. ORS is an effective way to replenish the fluids lost when a child has diarrhea and zinc tablets can reduce the duration and severity of a diarrhea bout. \\n\\n3. Using ORS and zinc in combination can save a child’s life.","languages":"","audience":""},{"position":8,"title":"Postnatal care services and their importance to your child","format":"Drama","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"1. Begin breastfeeding immediately after birth. And keep your baby in skin to skin contact on your bare chest and in between your breasts for at least the first one hour after birth.\\n\\n2. Breastfeed babies exclusively (without giving anything else including pito, porridge and water) for 6 months.\\n\\n3. Mother and new-born visit Child Welfare Clinics every month for weighing \\n\\n4. After home birth, mother and new-born visit the Health Centre as soon as possible.\\n\\n5. Mother and new-born sleep under an insecticide-treated net.","languages":"","audience":""}]},{"position":3,"title":"WASH","audience":"","messages":[{"position":1,"title":"Handwashing with soap and running water","format":"Drama","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"All family members, including children, need to wash their hands thoroughly with soap and water after any contact with faeces, before touching or preparing food, and before feeding children. Where soap is not available, a substitute, such as ash and water, can be used.\\n\\nWashing the hands with soap and water removes germs. Rinsing the fingers with water is not enough – both hands need to be rubbed together with soap and water, and then rinsed with water.","languages":"","audience":""}]},{"position":4,"title":"Child protection","audience":"","messages":[{"position":1,"title":"How to register your child at birth","format":"Song","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Go to the Municipal Assembly and ask for the registrar of births office. You can go along with your child's weighing card","languages":"","audience":""},{"position":2,"title":"Importance of birth registration and logistics","format":"Drama","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"1. Birth registration establishes the identity, nationality and date of birth of your child.\\n2. Your child will need it in the future when he or she is enrolling in school.\\n3. Birth registrations are free in Ghana within the first year of your baby’s life.\\n4. It will be costly to do birth registration if your child grows up and wants to register for the certificate.","languages":"","audience":""},{"position":3,"title":"Alternative ways to discipline a child","format":"Drama","default_category_code":"","variant":"en, dga","sdg_goal_id":null,"sdg_target":null,"sdg_target_id":null,"key_points":"Instead of corporal punishments, parents should:\\n\\n1. Talk with your partner about how you want to bring up your children. Don’t undermine each other. \\n\\n2. Listen to your children and respect their views; spend time with your kids. \\n\\n3. Tell your child clearly and firmly with a calm voice what they have done wrong. \\n\\n4. Let children know not only what they should not do but what they should do too. \\n\\n5. If their behavior has caused a problem, ask them how to make things better. With your help it could be positive for you both!","languages":"","audience":""}]}]},{"deploymentnumber":4,"startdate":"2022-01-01","enddate":"2022-06-30","deployment":"TEST-22-4","playlists":[]},{"deploymentnumber":5,"startdate":"2022-07-01","enddate":"2022-08-31","deployment":"TEST-22-5","playlists":[]},{"deploymentnumber":6,"startdate":"2022-09-01","enddate":"2022-12-31","deployment":"TEST-22-6","playlists":[],"deploymentname":"named"}]
'''

EXPECTED_TEST_DATA = '''[{"deploymentnumber": 1, "startdate": null, "enddate": null, "deployment": null, "playlists": [{"position": 1, "title": "Health", "audience": "", "messages": [{"position": 1, "title": "Breast milk is the best food for babies.", "format": "", "default_category_code": "Health", "variant": "HP", "sdg_goal_id": "4", "sdg_target": "1", "sdg_target_id": "4.1", "key_points": "Breast milk is the best food for your baby.", "languages": "", "audience": ""}, {"position": 2, "title": "Family Planning: song", "format": "", "default_category_code": "Health", "variant": "~HP", "sdg_goal_id": "5", "sdg_target": "3", "sdg_target_id": "5.3", "key_points": "Family planning is good for you, for your family, and for your community.", "languages": "en", "audience": ""}, {"position": 3, "title": "Neo-natal care: Immunizations, Breastfeeding, Handwashing", "format": "", "default_category_code": "Health", "variant": "", "sdg_goal_id": "6", "sdg_target": null, "sdg_target_id": null, "key_points": "Take good care of your infant.", "languages": "en", "audience": ""}, {"position": 4, "title": "ANC benefit: song", "format": "", "default_category_code": "Health", "variant": "", "sdg_goal_id": "1", "sdg_target": "1", "sdg_target_id": "1.1", "key_points": "It's good to take good care of your infant.", "languages": "", "audience": ""}, {"position": 5, "title": "Mansugo can cause serious health problems", "format": "", "default_category_code": "Health", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Mansugo is bad stuff.", "languages": "", "audience": ""}, {"position": 6, "title": "Breastfeeding your newborn.", "format": "", "default_category_code": "Health", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "How to breastfeed.", "languages": "", "audience": ""}]}, {"position": 2, "title": "Agriculture", "audience": "Farmers", "messages": [{"position": 1, "title": "No-mounds: Plant your crops in rows instead of mounds.", "format": "", "default_category_code": "", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Increase yields and reduce erosion.", "languages": "", "audience": "Farmers"}, {"position": 2, "title": "Test your seeds before sowing, and use the proper number per hole.", "format": "", "default_category_code": "", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Test seeds so you know if they're good.", "languages": "", "audience": "Farmers"}, {"position": 3, "title": "Timing is important in planting and fertilizing", "format": "", "default_category_code": "", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "For every thing there is a season.", "languages": "", "audience": "Farmers"}]}, {"position": 3, "title": "Livestock", "audience": "", "messages": [{"position": 1, "title": "Vacinate livestock during the dry season, and house them to save their manure.", "format": "", "default_category_code": "Livestock", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "And a time for every thing.", "languages": "", "audience": ""}]}]}, {"deploymentnumber": 2, "startdate": null, "enddate": null, "deployment": null, "playlists": [{"position": 1, "title": "Intro Message", "audience": "", "messages": [{"position": 1, "title": "What's new in Deployment 2", "format": "", "default_category_code": "", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "What to expect", "languages": "", "audience": ""}]}, {"position": 2, "title": "Agriculture", "audience": "", "messages": [{"position": 1, "title": "No-mounds: Plant your crops in rows instead of mounds.", "format": "", "default_category_code": "", "variant": "~sc", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Increase yields and reduce erosion.", "languages": "en", "audience": ""}, {"position": 2, "title": "Test your seeds before sowing, and use the proper number per hole.", "format": "", "default_category_code": "", "variant": "~sc", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Test seeds so you know if they're good.", "languages": "en", "audience": ""}, {"position": 3, "title": "Timing is important in planting and fertilizing", "format": "", "default_category_code": "", "variant": "~sc", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "For every thing there is a season.", "languages": "en", "audience": ""}]}, {"position": 3, "title": "Health", "audience": "", "messages": [{"position": 1, "title": "Breast milk is the best food for babies.", "format": "", "default_category_code": "Health", "variant": "~sc", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Breast milk is the best food for your baby.", "languages": "en", "audience": ""}, {"position": 2, "title": "Family Planning: song", "format": "", "default_category_code": "Health", "variant": "~sc", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Family planning is good for you, for your family, and for your community.", "languages": "en", "audience": ""}, {"position": 3, "title": "Neo-natal care: Immunizations, Breastfeeding, Handwashing", "format": "", "default_category_code": "Health", "variant": "~sc", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Take good care of your infant.", "languages": "en", "audience": ""}, {"position": 4, "title": "ANC benefit: song", "format": "", "default_category_code": "Health", "variant": "~sc", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "It's good to take good care of your infant.", "languages": "en", "audience": ""}, {"position": 5, "title": "Mansugo can cause serious health problems", "format": "", "default_category_code": "Health", "variant": "~sc", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Mansugo is bad stuff.", "languages": "en", "audience": ""}, {"position": 6, "title": "Breastfeeding your newborn.", "format": "", "default_category_code": "Health", "variant": "~sc", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "How to breastfeed.", "languages": "en", "audience": ""}]}, {"position": 4, "title": "Livestock", "audience": "", "messages": [{"position": 1, "title": "Vacinate livestock during the dry season, and house them to save their manure.", "format": "", "default_category_code": "Livestock", "variant": "~sc", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "And a time for every thing.", "languages": "en", "audience": ""}]}, {"position": 5, "title": "Sound Check", "audience": "", "messages": [{"position": 1, "title": "Pink noise", "format": "", "default_category_code": "Music", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Random sound that seems to be as loud at every frequency.", "languages": "", "audience": ""}, {"position": 2, "title": "a220Hz", "format": "", "default_category_code": "Music", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "A low A.", "languages": "", "audience": ""}, {"position": 3, "title": "a440Hz", "format": "", "default_category_code": "Music", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "A middle A.", "languages": "", "audience": ""}, {"position": 4, "title": "1kHz", "format": "", "default_category_code": "Music", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "One thousand hertz.", "languages": "", "audience": ""}, {"position": 5, "title": "2kHz", "format": "", "default_category_code": "Music", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Two thousand hertz.", "languages": "", "audience": ""}, {"position": 6, "title": "4kHz", "format": "", "default_category_code": "Music", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Four thousand hertz.", "languages": "", "audience": ""}, {"position": 7, "title": "Silence", "format": "", "default_category_code": "Music", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "And in the naked light I saw\nTen thousand people, maybe more\nPeople talking without speaking\nPeople hearing without listening\nPeople writing songs that voices never share\nNo one dared\nDisturb the sound of silence", "languages": "", "audience": ""}, {"position": 8, "title": "Hip Hop", "format": "Song", "default_category_code": "Music", "variant": "", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Pounding bass. Well, actually The High Violets.", "languages": "", "audience": ""}]}]}, {"deploymentnumber": 3, "startdate": null, "enddate": null, "deployment": null, "playlists": [{"position": 1, "title": "Antenatal Clinics", "audience": "", "messages": [{"position": 1, "title": "Delivery with a skilled attendant is good for you, the baby and the community", "format": "Drama", "default_category_code": "Health", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Skilled attendants are accredited health professionals such as midwives, doctors, or nurses who have been educated and trained to proficiency in the skills needed to manage normal pregnancies, childbirth and the immediate postnatal period.\n\nChildbirth with a skilled attendant reduces the risk that the mother and/or the baby will die or experience complications from childbirth.\n\nDelivery at home might be comfortable, but delivery at a facility with a skilled attendant is the best way to ensure that you and your baby are healthy and will remain healthy.", "languages": "", "audience": ""}, {"position": 2, "title": "Birth preparedness and planning", "format": "Interview", "default_category_code": "Health", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Begin planning how you will deliver with a skilled attendant as soon as you know you are pregnant. And register under the NHIS.", "languages": "", "audience": ""}, {"position": 3, "title": "Pregnant women should deliver with a skilled attendant at a healthcare", "format": "Song", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Remember\u2026\n\u2022 Always deliver at a health facility with a skilled birth attendant.\n\u2022 A women should go for ANC at least 4 times during her pregnancy.", "languages": "", "audience": ""}, {"position": 4, "title": "Detecting labour signs and delivery with a skilled birth attendant", "format": "Drama", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "If you notice any of the following signs; visit the hospital immediately:\nSudden uncontrolled flow of water from the vagina, so called Breaking of waters\nRegular and strong contraction\nYou notice a discharge\nYou feel more cramps and increased back pain\nYou experience any bleeding or bright-red discharge (not brown or pinkish).\nYou experience blurred or double vision, a severe headache, or sudden swelling.\n\nThese can be symptoms of preeclampsia, which is characterized by pregnancy-induced high blood pressure and requires medical attention. \n\nOther preparations include planning ahead, having health insurance coverage and attending antenatal care clinic early and regularly should be included", "languages": "", "audience": ""}]}, {"position": 2, "title": "Child Welfare Clinics", "audience": "", "messages": [{"position": 1, "title": "Exclusive breastfeeding for six months", "format": "Song", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "\u2022 Give your baby ONLY breast milk for the first six months of his or her life; this means no water, other liquid or foods, not even a little.", "languages": "", "audience": ""}, {"position": 2, "title": "The healthiest babies are ones who are exclusively breastfed", "format": "Drama", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Remember\u2026\n\u2022 Give your baby ONLY breast milk for the first six months of his or her life; this means no water, no other liquid or foods, not even a little.\n\u2022 Breast milk alone is the best food for baby under six months old", "languages": "", "audience": ""}, {"position": 3, "title": "Reducing chances of getting malaria", "format": "Song", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Sleep under an LLIN each night to reduce your chances of getting malaria. Weed around your house and take away stagnant water, they produces mosquitoes. Remember, mosquitoe bites gives us malaria.", "languages": "", "audience": ""}, {"position": 4, "title": "Malaria and how it is transmitted", "format": "Interview", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Malaria is an illness caused by the presence of malarial parasite in the human body. It spreads through the bite of mosquito. When a mosquito bites a person suffering from malaria, the parasite enters the body of the mosquito. When the infected mosquito bites a healthy person then he/she may develop malaria.", "languages": "", "audience": ""}, {"position": 5, "title": "Using ORS and Zinc tablets to treat diarrhea in children", "format": "Song", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "1. Diarrhea is a leading killer of children. It kills children by draining liquid from the body, dehydrating the child.\n\n2. ORS is an effective way to replenish the fluids lost when a child has diarrhea and zinc tablets can reduce the duration and severity of a diarrhea bout. \n\n3. Using ORS and zinc in combination can save a child\u2019s life.", "languages": "", "audience": ""}, {"position": 6, "title": "Signs of diarrhoea and using ORS and Zinc tablets as treatment options", "format": "Interview", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Remember\u2026\n\u2022 It is diarrhoea if your child or baby passes more than three watery stools in a day. If you do not treat this, your baby can die from diarrhoea.\n\u2022 ORS is a very effective way to treat diarrhoea in your children. \n\u2022 Give your child zinc tablets for 10 days, even if the diarrhoea has stopped, to prevent diarrhoea for 3 months.", "languages": "", "audience": ""}, {"position": 7, "title": "Cause, effects and treatment of diarrhoea", "format": "Song", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "1. Diarrhea is a leading killer of children. It kills children by draining liquid from the body, dehydrating the child.\n\n2. ORS is an effective way to replenish the fluids lost when a child has diarrhea and zinc tablets can reduce the duration and severity of a diarrhea bout. \n\n3. Using ORS and zinc in combination can save a child\u2019s life.", "languages": "", "audience": ""}, {"position": 8, "title": "Postnatal care services and their importance to your child", "format": "Drama", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "1. Begin breastfeeding immediately after birth. And keep your baby in skin to skin contact on your bare chest and in between your breasts for at least the first one hour after birth.\n\n2. Breastfeed babies exclusively (without giving anything else including pito, porridge and water) for 6 months.\n\n3. Mother and new-born visit Child Welfare Clinics every month for weighing \n\n4. After home birth, mother and new-born visit the Health Centre as soon as possible.\n\n5. Mother and new-born sleep under an insecticide-treated net.", "languages": "", "audience": ""}]}, {"position": 3, "title": "WASH", "audience": "", "messages": [{"position": 1, "title": "Handwashing with soap and running water", "format": "Drama", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "All family members, including children, need to wash their hands thoroughly with soap and water after any contact with faeces, before touching or preparing food, and before feeding children. Where soap is not available, a substitute, such as ash and water, can be used.\n\nWashing the hands with soap and water removes germs. Rinsing the fingers with water is not enough \u2013 both hands need to be rubbed together with soap and water, and then rinsed with water.", "languages": "", "audience": ""}]}, {"position": 4, "title": "Child protection", "audience": "", "messages": [{"position": 1, "title": "How to register your child at birth", "format": "Song", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Go to the Municipal Assembly and ask for the registrar of births office. You can go along with your child's weighing card", "languages": "", "audience": ""}, {"position": 2, "title": "Importance of birth registration and logistics", "format": "Drama", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "1. Birth registration establishes the identity, nationality and date of birth of your child.\n2. Your child will need it in the future when he or she is enrolling in school.\n3. Birth registrations are free in Ghana within the first year of your baby\u2019s life.\n4. It will be costly to do birth registration if your child grows up and wants to register for the certificate.", "languages": "", "audience": ""}, {"position": 3, "title": "Alternative ways to discipline a child", "format": "Drama", "default_category_code": "", "variant": "en, dga", "sdg_goal_id": null, "sdg_target": null, "sdg_target_id": null, "key_points": "Instead of corporal punishments, parents should:\n\n1. Talk with your partner about how you want to bring up your children. Don\u2019t undermine each other. \n\n2. Listen to your children and respect their views; spend time with your kids. \n\n3. Tell your child clearly and firmly with a calm voice what they have done wrong. \n\n4. Let children know not only what they should not do but what they should do too. \n\n5. If their behavior has caused a problem, ask them how to make things better. With your help it could be positive for you both!", "languages": "", "audience": ""}]}]}]'''
test_data = [
    {
        "deploymentnumber": 1,
        "startdate": "None",
        "enddate": "None",
        "deployment": "None",
        "playlists": [
            {
                "position": 1,
                "title": "Health",
                "audience": "",
                "messages": [
                    {
                        "position": 1,
                        "title": "Breast milk is the best food for babies.",
                        "format": "",
                        "default_category_code": "Health",
                        "variant": "HP",
                        "sdg_goal_id": "4",
                        "sdg_target": "1",
                        "sdg_target_id": "4.1",
                        "key_points": "Breast milk is the best food for your baby.",
                        "languages": ""
                    },
                    {
                        "position": 2,
                        "title": "Family Planning: song",
                        "format": "",
                        "default_category_code": "Health",
                        "variant": "~HP",
                        "sdg_goal_id": "5",
                        "sdg_target": "3",
                        "sdg_target_id": "5.3",
                        "key_points": "Family planning is good for you, for your family, and for your community.",
                        "languages": "en"
                    },
                    {
                        "position": 3,
                        "title": "Neo-natal care: Immunizations, Breastfeeding, Handwashing",
                        "format": "",
                        "default_category_code": "Health",
                        "variant": "",
                        "sdg_goal_id": "6",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Take good care of your infant.",
                        "languages": "en"
                    },
                    {
                        "position": 4,
                        "title": "ANC benefit: song",
                        "format": "",
                        "default_category_code": "Health",
                        "variant": "",
                        "sdg_goal_id": "1",
                        "sdg_target": "1",
                        "sdg_target_id": "1.1",
                        "key_points": "It's good to take good care of your infant.",
                        "languages": ""
                    },
                    {
                        "position": 5,
                        "title": "Mansugo can cause serious health problems",
                        "format": "",
                        "default_category_code": "Health",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Mansugo is bad stuff.",
                        "languages": ""
                    },
                    {
                        "position": 6,
                        "title": "Breastfeeding your newborn.",
                        "format": "",
                        "default_category_code": "Health",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "How to breastfeed.",
                        "languages": ""
                    }
                ]
            },
            {
                "position": 2,
                "title": "Agriculture",
                "audience": "Farmers.",
                "messages": [
                    {
                        "position": 1,
                        "title": "No-mounds: Plant your crops in rows instead of mounds.",
                        "format": "",
                        "default_category_code": "",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Increase yields and reduce erosion.",
                        "languages": ""
                    },
                    {
                        "position": 2,
                        "title": "Test your seeds before sowing, and use the proper number per hole.",
                        "format": "",
                        "default_category_code": "",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Test seeds so you know if they're good.",
                        "languages": ""
                    },
                    {
                        "position": 3,
                        "title": "Timing is important in planting and fertilizing",
                        "format": "",
                        "default_category_code": "",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "For every thing there is a season.",
                        "languages": ""
                    }
                ]
            },
            {
                "position": 3,
                "title": "Livestock",
                "audience": "",
                "messages": [
                    {
                        "position": 1,
                        "title": "Vacinate livestock during the dry season, and house them to save their manure.",
                        "format": "",
                        "default_category_code": "Livestock",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "And a time for every thing.",
                        "languages": ""
                    }
                ]
            }
        ]
    },
    {
        "deploymentnumber": 2,
        "startdate": "None",
        "enddate": "None",
        "deployment": "None",
        "playlists": [
            {
                "position": 1,
                "title": "Intro Message",
                "audience": "",
                "messages": [
                    {
                        "position": 1,
                        "title": "What's new in Deployment 2",
                        "format": "",
                        "default_category_code": "",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "What to expect",
                        "languages": ""
                    }
                ]
            },
            {
                "position": 2,
                "title": "Agriculture",
                "audience": "",
                "messages": [
                    {
                        "position": 1,
                        "title": "No-mounds: Plant your crops in rows instead of mounds.",
                        "format": "",
                        "default_category_code": "",
                        "variant": "~sc",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Increase yields and reduce erosion.",
                        "languages": "en"
                    },
                    {
                        "position": 2,
                        "title": "Test your seeds before sowing, and use the proper number per hole.",
                        "format": "",
                        "default_category_code": "",
                        "variant": "~sc",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Test seeds so you know if they're good.",
                        "languages": "en"
                    },
                    {
                        "position": 3,
                        "title": "Timing is important in planting and fertilizing",
                        "format": "",
                        "default_category_code": "",
                        "variant": "~sc",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "For every thing there is a season.",
                        "languages": "en"
                    }
                ]
            },
            {
                "position": 3,
                "title": "Health",
                "audience": "",
                "messages": [
                    {
                        "position": 1,
                        "title": "Breast milk is the best food for babies.",
                        "format": "",
                        "default_category_code": "Health",
                        "variant": "~sc",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Breast milk is the best food for your baby.",
                        "languages": "en"
                    },
                    {
                        "position": 2,
                        "title": "Family Planning: song",
                        "format": "",
                        "default_category_code": "Health",
                        "variant": "~sc",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Family planning is good for you, for your family, and for your community.",
                        "languages": "en"
                    },
                    {
                        "position": 3,
                        "title": "Neo-natal care: Immunizations, Breastfeeding, Handwashing",
                        "format": "",
                        "default_category_code": "Health",
                        "variant": "~sc",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Take good care of your infant.",
                        "languages": "en"
                    },
                    {
                        "position": 4,
                        "title": "ANC benefit: song",
                        "format": "",
                        "default_category_code": "Health",
                        "variant": "~sc",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "It's good to take good care of your infant.",
                        "languages": "en"
                    },
                    {
                        "position": 5,
                        "title": "Mansugo can cause serious health problems",
                        "format": "",
                        "default_category_code": "Health",
                        "variant": "~sc",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Mansugo is bad stuff.",
                        "languages": "en"
                    },
                    {
                        "position": 6,
                        "title": "Breastfeeding your newborn.",
                        "format": "",
                        "default_category_code": "Health",
                        "variant": "~sc",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "How to breastfeed.",
                        "languages": "en"
                    }
                ]
            },
            {
                "position": 4,
                "title": "Livestock",
                "audience": "",
                "messages": [
                    {
                        "position": 1,
                        "title": "Vacinate livestock during the dry season, and house them to save their manure.",
                        "format": "",
                        "default_category_code": "Livestock",
                        "variant": "~sc",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "And a time for every thing.",
                        "languages": "en"
                    }
                ]
            },
            {
                "position": 5,
                "title": "Sound Check",
                "audience": "",
                "messages": [
                    {
                        "position": 1,
                        "title": "Pink noise",
                        "format": "",
                        "default_category_code": "Music",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Random sound that seems to be as loud at every frequency.",
                        "languages": ""
                    },
                    {
                        "position": 2,
                        "title": "a220Hz",
                        "format": "",
                        "default_category_code": "Music",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "A low A.",
                        "languages": ""
                    },
                    {
                        "position": 3,
                        "title": "a440Hz",
                        "format": "",
                        "default_category_code": "Music",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "A middle A.",
                        "languages": ""
                    },
                    {
                        "position": 4,
                        "title": "1kHz",
                        "format": "",
                        "default_category_code": "Music",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "One thousand hertz.",
                        "languages": ""
                    },
                    {
                        "position": 5,
                        "title": "2kHz",
                        "format": "",
                        "default_category_code": "Music",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Two thousand hertz.",
                        "languages": ""
                    },
                    {
                        "position": 6,
                        "title": "4kHz",
                        "format": "",
                        "default_category_code": "Music",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Four thousand hertz.",
                        "languages": ""
                    },
                    {
                        "position": 7,
                        "title": "Silence",
                        "format": "",
                        "default_category_code": "Music",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "And in the naked light I saw\nTen thousand people, maybe more\nPeople talking without speaking\nPeople hearing without listening\nPeople writing songs that voices never share\nNo one dared\nDisturb the sound of silence",
                        "languages": ""
                    },
                    {
                        "position": 8,
                        "title": "Hip Hop",
                        "format": "Song",
                        "default_category_code": "Music",
                        "variant": "",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Pounding bass. Well, actually The High Violets.",
                        "languages": ""
                    }
                ]
            }
        ]
    },
    {
        "deploymentnumber": 3,
        "startdate": "None",
        "enddate": "None",
        "deployment": "None",
        "playlists": [
            {
                "position": 1,
                "title": "Antenatal Clinics",
                "audience": "",
                "messages": [
                    {
                        "position": 1,
                        "title": "Delivery with a skilled attendant is good for you, the baby and the community",
                        "format": "Drama",
                        "default_category_code": "Health",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Skilled attendants are accredited health professionals such as midwives, doctors, or nurses who have been educated and trained to proficiency in the skills needed to manage normal pregnancies, childbirth and the immediate postnatal period.\n\nChildbirth with a skilled attendant reduces the risk that the mother and/or the baby will die or experience complications from childbirth.\n\nDelivery at home might be comfortable, but delivery at a facility with a skilled attendant is the best way to ensure that you and your baby are healthy and will remain healthy.",
                        "languages": ""
                    },
                    {
                        "position": 2,
                        "title": "Birth preparedness and planning",
                        "format": "Interview",
                        "default_category_code": "Health",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Begin planning how you will deliver with a skilled attendant as soon as you know you are pregnant. And register under the NHIS.",
                        "languages": ""
                    },
                    {
                        "position": 3,
                        "title": "Pregnant women should deliver with a skilled attendant at a healthcare",
                        "format": "Song",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Remember…\n• Always deliver at a health facility with a skilled birth attendant.\n• A women should go for ANC at least 4 times during her pregnancy.",
                        "languages": ""
                    },
                    {
                        "position": 4,
                        "title": "Detecting labour signs and delivery with a skilled birth attendant",
                        "format": "Drama",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "If you notice any of the following signs; visit the hospital immediately:\nSudden uncontrolled flow of water from the vagina, so called Breaking of waters\nRegular and strong contraction\nYou notice a discharge\nYou feel more cramps and increased back pain\nYou experience any bleeding or bright-red discharge (not brown or pinkish).\nYou experience blurred or double vision, a severe headache, or sudden swelling.\n\nThese can be symptoms of preeclampsia, which is characterized by pregnancy-induced high blood pressure and requires medical attention. \n\nOther preparations include planning ahead, having health insurance coverage and attending antenatal care clinic early and regularly should be included",
                        "languages": ""
                    }
                ]
            },
            {
                "position": 2,
                "title": "Child Welfare Clinics",
                "audience": "",
                "messages": [
                    {
                        "position": 1,
                        "title": "Exclusive breastfeeding for six months",
                        "format": "Song",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "• Give your baby ONLY breast milk for the first six months of his or her life; this means no water, other liquid or foods, not even a little.",
                        "languages": ""
                    },
                    {
                        "position": 2,
                        "title": "The healthiest babies are ones who are exclusively breastfed",
                        "format": "Drama",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Remember…\n• Give your baby ONLY breast milk for the first six months of his or her life; this means no water, no other liquid or foods, not even a little.\n• Breast milk alone is the best food for baby under six months old",
                        "languages": ""
                    },
                    {
                        "position": 3,
                        "title": "Reducing chances of getting malaria",
                        "format": "Song",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Sleep under an LLIN each night to reduce your chances of getting malaria. Weed around your house and take away stagnant water, they produces mosquitoes. Remember, mosquitoe bites gives us malaria.",
                        "languages": ""
                    },
                    {
                        "position": 4,
                        "title": "Malaria and how it is transmitted",
                        "format": "Interview",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Malaria is an illness caused by the presence of malarial parasite in the human body. It spreads through the bite of mosquito. When a mosquito bites a person suffering from malaria, the parasite enters the body of the mosquito. When the infected mosquito bites a healthy person then he/she may develop malaria.",
                        "languages": ""
                    },
                    {
                        "position": 5,
                        "title": "Using ORS and Zinc tablets to treat diarrhea in children",
                        "format": "Song",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "1. Diarrhea is a leading killer of children. It kills children by draining liquid from the body, dehydrating the child.\n\n2. ORS is an effective way to replenish the fluids lost when a child has diarrhea and zinc tablets can reduce the duration and severity of a diarrhea bout. \n\n3. Using ORS and zinc in combination can save a child’s life.",
                        "languages": ""
                    },
                    {
                        "position": 6,
                        "title": "Signs of diarrhoea and using ORS and Zinc tablets as treatment options",
                        "format": "Interview",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Remember…\n• It is diarrhoea if your child or baby passes more than three watery stools in a day. If you do not treat this, your baby can die from diarrhoea.\n• ORS is a very effective way to treat diarrhoea in your children. \n• Give your child zinc tablets for 10 days, even if the diarrhoea has stopped, to prevent diarrhoea for 3 months.",
                        "languages": ""
                    },
                    {
                        "position": 7,
                        "title": "Cause, effects and treatment of diarrhoea",
                        "format": "Song",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "1. Diarrhea is a leading killer of children. It kills children by draining liquid from the body, dehydrating the child.\n\n2. ORS is an effective way to replenish the fluids lost when a child has diarrhea and zinc tablets can reduce the duration and severity of a diarrhea bout. \n\n3. Using ORS and zinc in combination can save a child’s life.",
                        "languages": ""
                    },
                    {
                        "position": 8,
                        "title": "Postnatal care services and their importance to your child",
                        "format": "Drama",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "1. Begin breastfeeding immediately after birth. And keep your baby in skin to skin contact on your bare chest and in between your breasts for at least the first one hour after birth.\n\n2. Breastfeed babies exclusively (without giving anything else including pito, porridge and water) for 6 months.\n\n3. Mother and new-born visit Child Welfare Clinics every month for weighing \n\n4. After home birth, mother and new-born visit the Health Centre as soon as possible.\n\n5. Mother and new-born sleep under an insecticide-treated net.",
                        "languages": ""
                    }
                ]
            },
            {
                "position": 3,
                "title": "WASH",
                "audience": "",
                "messages": [
                    {
                        "position": 1,
                        "title": "Handwashing with soap and running water",
                        "format": "Drama",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "All family members, including children, need to wash their hands thoroughly with soap and water after any contact with faeces, before touching or preparing food, and before feeding children. Where soap is not available, a substitute, such as ash and water, can be used.\n\nWashing the hands with soap and water removes germs. Rinsing the fingers with water is not enough – both hands need to be rubbed together with soap and water, and then rinsed with water.",
                        "languages": ""
                    }
                ]
            },
            {
                "position": 4,
                "title": "Child protection",
                "audience": "",
                "messages": [
                    {
                        "position": 1,
                        "title": "How to register your child at birth",
                        "format": "Song",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Go to the Municipal Assembly and ask for the registrar of births office. You can go along with your child's weighing card",
                        "languages": ""
                    },
                    {
                        "position": 2,
                        "title": "Importance of birth registration and logistics",
                        "format": "Drama",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "1. Birth registration establishes the identity, nationality and date of birth of your child.\n2. Your child will need it in the future when he or she is enrolling in school.\n3. Birth registrations are free in Ghana within the first year of your baby’s life.\n4. It will be costly to do birth registration if your child grows up and wants to register for the certificate.",
                        "languages": ""
                    },
                    {
                        "position": 3,
                        "title": "Alternative ways to discipline a child",
                        "format": "Drama",
                        "default_category_code": "",
                        "variant": "en, dga",
                        "sdg_goal_id": "None",
                        "sdg_target": "None",
                        "sdg_target_id": "None",
                        "key_points": "Instead of corporal punishments, parents should:\n\n1. Talk with your partner about how you want to bring up your children. Don’t undermine each other. \n\n2. Listen to your children and respect their views; spend time with your kids. \n\n3. Tell your child clearly and firmly with a calm voice what they have done wrong. \n\n4. Let children know not only what they should not do but what they should do too. \n\n5. If their behavior has caused a problem, ask them how to make things better. With your help it could be positive for you both!",
                        "languages": ""
                    }
                ]
            }
        ]
    }
]

test_json = json.dumps(test_data)


def init(_args: Namespace, engine=None):
    global args, _engine
    args = _args
    if engine is not None:
        _engine = engine


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = db.get_db_engine()
    return _engine


def _db_migrate(conn: Connection):
    v1 = '''
        ALTER TABLE messages ADD COLUMN languages CHARACTER VARYING;
        ALTER TABLE messages ADD COLUMN audience CHARACTER VARYING;
        
        CREATE TEMPORARY TABLE langs AS (
                  SELECT m.id, STRING_AGG(DISTINCT language_code,',') AS languages 
                    FROM messages m 
                    JOIN message_languages l ON l.message_id=m.id 
                   GROUP BY m.id
                );
        
        UPDATE messages m
           SET languages = ( SELECT languages from langs l where l.id=m.id);
        
        UPDATE messages m
           SET audience = ( SELECT audience from playlists p where p.id=m.playlist_id);
        
        SELECT program_id, title, audience 
          FROM messages 
         WHERE audience IS NOT NULL 
         LIMIT 5;
        
        SELECT DISTINCT m.title, languages, STRING_AGG(DISTINCT language_code,',') 
          FROM messages m 
          JOIN message_languages l ON l.message_id=m.id 
         GROUP BY m.id, m.title, m.languages
         LIMIT 5;
        
        DROP TABLE message_languages;
        ALTER TABLE playlists DROP COLUMN audience;        
        '''
    v2 = '''
        create temp view sd_targets as 
            select sdg.id as sdg_id, subsection as target_id, sdg.section::text || '.' || target.subsection::text as label, target.label as description
                from sustainable_development_goals sdg
                join sustainable_development_targets target
                  on target.goal_id = sdg.id ;
    '''
    v3 = '''
        create temp view content as 
            select pl.program_id as project
                    ,depl.id as deployment_id
                    ,depl.deploymentnumber::integer as deployment_num
                    ,pl.title as playlist_title
                    ,pl.position as playlist_pos
                    ,pl.id as playlist_id
                    ,msg.title as message_title
                    ,msg.position as message_pos
                    ,msg.id as message_id
                    ,msg.key_points
                    ,scat.fullname as default_category
                    ,msg.format
                    ,pl.audience
                    ,msg.variant
                    ,sdg.sdg_goal_id as sdg_goals
                    ,sdt.sdg_target_id as sdg_targets
                    ,STRING_AGG(distinct ml.language_code, ',') as languagecode

            from playlists pl
            join deployments depl
              on pl.deployment_id = depl.id
            left outer join messages msg
              on pl.program_id = msg.program_id AND msg.playlist_id = pl.id
            left outer join sdg_goals sdg
              on msg.sdg_goal_id = sdg.sdg_goal_id
            left outer join sdg_targets sdt
              on msg.sdg_target = sdt.sdg_target and msg.sdg_goal_id = sdt.sdg_goal_id
            left outer join message_languages ml
              on ml.message_id = msg.id
            left outer join supportedcategories scat
              on msg.default_category_code = scat.categorycode

            group by pl.program_id
                    ,depl.deploymentnumber
                    ,depl.id
                    ,pl.position
                    ,pl.title
                    ,pl.id
                    ,msg.position
                    ,msg.title
                    ,msg.id
                    ,msg.key_points
                    ,scat.fullname
                    ,msg.format
                    ,pl.audience
                    ,msg.variant
                    ,sdg.sdg_goal_id
                    ,sdt.sdg_target_id


            order by pl.program_id, depl.deploymentnumber, pl.position, msg.position;
    '''
    result = conn.execute(text(v1))
    print('\n')


def test_other():
    global args
    has_deployment_id = db.table_has_column('playlists', 'deployment_id')
    engine = get_engine()

    program = 'TEST'  # args.programs[0]

    update = json.loads(test_json)

    program_spec: Spec.Program = Spec.progspec_from_json(program, update)

    importer: ImportProcessor.ImportProcessor = ImportProcessor.ImportProcessor(program, program_spec)

    with db.get_db_connection(engine=engine) as conn:
        commit = args.disposition == 'commit'
        transaction = conn.begin()
        importer.update_db_program_spec(conn, content_only=True)
        if commit:
            transaction.commit()
            print(f'Changes commited for {program}')
        else:
            transaction.rollback()
            print(f'Changes rolled back for {program}')


def test_from_json(programid: str):
    from_json = json.loads(JSON_STR)
    spec: Spec.Program = Spec.progspec_from_json(programid, from_json)
    print(spec)


def test_get_put(programid: str):
    global args
    email = 'bill@amplio.org'
    event = {
        'requestContext': {
            'authorizer': {'claims': {'email': email}}
        },
        'queryStringParameters': {'programid': programid, 'return_diff': True},
        'pathParameters': {'proxy': 'get_content'},
        'httpMethod': 'GET',
    }
    context = {}
    result = SpecHandler.lambda_router(event, context)
    content = json.loads(result.get('body'))
    depl1 = content[0]
    pl1 = depl1.get('playlists')[0]
    msg1 = pl1.get('messages')[0]
    msg1['audience'] = 'Everyone'

    event['body'] = json.dumps(content)
    event['pathParameters']['proxy'] = 'put_content'
    event['httpMethod'] = 'POST'

    result2 = SpecHandler.lambda_router(event, context)
    print(result2)


# subclass JSONEncoder
class DateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()


def run_tests(programid: str):
    test_from_json(programid)
    engine = get_engine()
    exporter = XlsExporter.Exporter(programid, engine)
    exporter.read_from_database()
    content = Spec.progspec_to_json(exporter.program_spec)
    content_str = json.dumps(content, cls=DateTimeEncoder)
    with open('expected_data.bin', 'r') as inf:
        expected_str = inf.read()

    if content_str != expected_str:
        with open('found_data.bin', 'w') as outf:
            outf.write(content_str)
        print("Test data doesn't match")

    depl1 = content[0]
    pl1 = depl1.get('playlists')[0]
    msg1 = pl1.get('messages')[0]
    msg1['audience'] = 'Nurses'

    spec: Spec.Program = Spec.progspec_from_json(programid, content)

    differ: SpecCompare.SpecCompare = SpecCompare.SpecCompare(exporter.program_spec, spec)
    diffs = differ.diff()
    print(diffs)

    # importer: ImportProcessor = ImportProcessor(programid, spec)
    # with db.get_db_connection(engine=engine) as conn:
    #     commit = args.disposition == 'commit'
    #     transaction = conn.begin()
    #     importer.update_db_program_spec(conn, content_only=True)
    #     if commit:
    #         transaction.commit()
    #         print(f'Changes commited for {programid}')
    #     else:
    #         transaction.rollback()
    #         print(f'Changes rolled back for {programid}')
    #
    # print(len(content))

    test_get_put(programid)
