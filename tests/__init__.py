#TODO für die tests !!!
# in den klassen bzw. methoden sind viele stellen, welche nicht ohne mock getestet werden können.
# der ganze ablauf soll auch nciht in folge eines unittests geschehen, das ist richtig.
# es muss aber in allen job und processing klassen geschaut werden, ob in den methodn teile
# ausgelagert werden können, damit diese in den unitests getestet werden könnnen.
# beispiel sind die ganzenn sql aufrufe und umwandeln von daten (auslagern!)


from . import test_anon_job
from . import test_anon_processing
from . import test_deanon_job
from . import test_deanon_processing
from . import test_main_job
from . import test_main_processing
from . import test_providers
from . import test_utils

from . import integrationstest_anonymization
