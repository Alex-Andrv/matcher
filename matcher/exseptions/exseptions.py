class UserInNotVoteMode(Exception):
    pass


class SMPTError(Exception):
    pass


class UserBuilderConvertError(Exception):
    """Can't convert buildr to user, because builder contain not set value"""

class CriterionBuilderConvertError(Exception):
    """Can't convert buildr to criterion, because builder contain not set value"""


class NotFoundInState(Exception):
    """Can't find data in state"""

class NotFoundInEdit(Exception):
    """Can't find data in edit"""


class UnexpectedCallback(Exception):
    """Unexpected callback"""
