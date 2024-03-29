from typing import List

from pyscipopt.scip import Model
from typeguard import check_type

from matcher.models.Criterion import Criterion, MeetingFormat
from matcher.models.Group import Group
from matcher.models.MyUser import MyUser
from matcher.models.WorkPlace import WorkPlace


class OpbTask:
    def __init__(self, users):
        self.users = users
        self.model = Model()
        self.vars = dict()

    def _get_var(self, t_user_id1: int, t_user_id2: int):

        check_type(t_user_id1, int)
        check_type(t_user_id2, int)

        key = f"{t_user_id1}_{t_user_id2}"
        if t_user_id1 > t_user_id2:
            key = f"{t_user_id2}_{t_user_id1}"
        if key not in self.vars:
            self.vars[key] = self.model.addVar(vtype="B", name=key, lb=0, ub=1)
        return self.vars[key]

    def _get_objective_function(self):
        objective = 0
        t_user_ids = list(self.users.keys())
        for i in range(len(t_user_ids)):
            for j in range(i + 1, len(t_user_ids)):
                var = self._get_var(t_user_ids[i], t_user_ids[j])
                criterion_user_1: Criterion = self.users[t_user_ids[i]]["criterion"]
                criterion_user_2: Criterion = self.users[t_user_ids[j]]["criterion"]
                coeff = len(set(criterion_user_1.interests) & set(criterion_user_2.interests)) + 1
                objective -= coeff * var
        return objective

    def _only_one_companion_constraints(self):
        t_user_ids = list(self.users.keys())
        constraints = []
        for i in range(len(t_user_ids)):
            constraint = 0
            for j in range(len(t_user_ids)):
                if i == j:
                    continue
                constraint += self._get_var(t_user_ids[i], t_user_ids[j]) * 1
            constraints.append(constraint <= 1)
        return constraints

    def _forbid_homies_constraints(self):
        constraints = []
        for t_user_id, user_dict in self.users.items():
            for homies_t_user_id in user_dict['homies']:
                var = self._get_var(t_user_id, homies_t_user_id)
                constraints.append(var <= 0)
        return constraints

    def _forbid_not_intersection_place_constraints(self):
        constraints = []
        t_user_ids = list(self.users.keys())
        for i in range(len(t_user_ids)):
            for j in range(i + 1, len(t_user_ids)):
                var = self._get_var(t_user_ids[i], t_user_ids[j])
                criterion_user_1: Criterion = self.users[t_user_ids[i]]["criterion"]
                criterion_user_2: Criterion = self.users[t_user_ids[j]]["criterion"]
                if criterion_user_1.meeting_format == MeetingFormat.OFFLINE and \
                    criterion_user_2.meeting_format == MeetingFormat.ONLINE:
                    constraints.append(var <= 0)

                if criterion_user_1.meeting_format == MeetingFormat.ONLINE and \
                    criterion_user_2.meeting_format == MeetingFormat.OFFLINE:
                    constraints.append(var <= 0)

                if criterion_user_1.meeting_format != MeetingFormat.ONLINE and \
                    criterion_user_2.meeting_format != MeetingFormat.ONLINE and \
                        not (criterion_user_1.meeting_format == MeetingFormat.ANY and
                             criterion_user_2.meeting_format == MeetingFormat.ANY):
                    if len(set(criterion_user_1.preferred_places).
                                   intersection(set(criterion_user_2.preferred_places))) == 0:
                        constraints.append(var <= 0)

        return constraints

    def _forbid_not_different_role_constraints(self):
        constraints = []
        t_user_ids = list(self.users.keys())
        for i in range(len(t_user_ids)):
            for j in range(i + 1, len(t_user_ids)):
                var = self._get_var(t_user_ids[i], t_user_ids[j])
                user1: MyUser = self.users[t_user_ids[i]]["user"]
                user2: MyUser = self.users[t_user_ids[j]]["user"]
                if user1.role != user2.role:
                    constraints.append(var <= 0)
        return constraints

    def _is_cross_groups(self, groups_user1: List[Group], groups_user2: List[Group]):
        set_groups_user1 = set(map(lambda group_user1: group_user1.name, groups_user1))
        set_groups_user2 = set(map(lambda group_user2: group_user2.name, groups_user2))
        return len(set_groups_user1 & set_groups_user2) != 0

    def _is_cross_works(self, works_user1: List[WorkPlace], works_user2: List[WorkPlace]):
        set_works_user1 = set(map(lambda work_user1: work_user1.name, works_user1))
        set_works_user2 = set(map(lambda work_user2: work_user2.name, works_user2))
        return len(set_works_user1 & set_works_user2) != 0

    def _forbid_same_group_or_departament_constraints(self):
        constraints = []
        t_user_ids = list(self.users.keys())
        for i in range(len(t_user_ids)):
            for j in range(i + 1, len(t_user_ids)):
                var = self._get_var(t_user_ids[i], t_user_ids[j])
                groups_user1: List[Group] = self.users[t_user_ids[i]]["groups"]
                groups_user2: List[Group] = self.users[t_user_ids[j]]["groups"]
                works_user1: List[WorkPlace] = self.users[t_user_ids[i]]["works"]
                works_user2: List[WorkPlace] = self.users[t_user_ids[j]]["works"]
                if self._is_cross_groups(groups_user1, groups_user2) or self._is_cross_works(works_user1, works_user2):
                    constraints.append(var <= 0)

        return constraints

    def _generate_task(self):
        self.model.setObjective(self._get_objective_function())
        [self.model.addCons(constraint) for constraint in self._only_one_companion_constraints()]
        [self.model.addCons(constraint) for constraint in self._forbid_homies_constraints()]
        [self.model.addCons(constraint) for constraint in self._forbid_not_intersection_place_constraints()]
        [self.model.addCons(constraint) for constraint in self._forbid_not_different_role_constraints()]
        [self.model.addCons(constraint) for constraint in self._forbid_same_group_or_departament_constraints()]

    def _get_matching(self, solution):
        matching = []
        t_user_ids = list(self.users.keys())
        used = set()
        for i in range(len(t_user_ids)):
            for j in range(i + 1, len(t_user_ids)):
                var = self._get_var(t_user_ids[i], t_user_ids[j])
                if solution[var]:
                    matching.append((t_user_ids[i], t_user_ids[j]))
                    assert t_user_ids[i] not in used, "t_user_ids[i] already in used"
                    assert t_user_ids[j] not in used, "t_user_ids[j] already in used"
                    used.add(t_user_ids[i])
                    used.add(t_user_ids[j])
        free_users = list(set(t_user_ids).difference(used))
        return free_users, matching


    def solve(self, time_limit=10000):
        self.model.setParam('limits/time', time_limit)
        self._generate_task()
        self.model.optimize()
        return self._get_matching(self.model.getBestSol())