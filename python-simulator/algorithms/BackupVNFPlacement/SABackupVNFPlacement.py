import random
import math
from typing import List, Tuple


class SABackupVNFPlacement:
    def __init__(
        self,
        policy,
        initial_temperature=100,
        cooling_rate=0.95,
        iterations_per_temperature=50,
        min_temperature=0.1,
    ):
        self.policy = policy
        self.system = self.policy.placement()
        self.candidate_nodes = sorted(
            self.system.nodes,
            key=lambda node: self.policy.get_node_score(node),
        )
        self.initial_temperature = initial_temperature
        self.cooling_rate = cooling_rate
        self.iterations_per_temperature = iterations_per_temperature
        self.min_temperature = min_temperature
        self.dimension = sum(len(sfc.vnfs) for sfc in self.system.sfcs)

    def initialize_solution(self) -> List[int]:
        return [random.randint(1, 2) for _ in range(self.dimension)]

    def fitness(self, solution: List[int]) -> float:
        self.system.reset()
        self.policy.placement()

        idx = 0
        reward = 0
        processed_vnfs = set()

        print("Solution:", solution)
        candidate_nodes = self.candidate_nodes.copy()

        for sfc in self.system.sfcs:
            primary_vnfs = [vnf for vnf in sfc.vnfs if vnf.backup_of == 0]

            for vnf in primary_vnfs:
                # Skip if VNF already processed
                if vnf.name in processed_vnfs:
                    continue

                # Safely get number of redundant instances
                if idx >= len(solution):
                    break

                redundant_instances = int(solution[idx])
                idx += 1
                processed_vnfs.add(vnf.name)

                if redundant_instances == 0:
                    continue

                # Try to place redundant VNFs
                success, candidate_nodes = self.system.place_redundant_vnf(
                    vnf, redundant_instances, candidate_nodes, sfc
                )

                if not success:
                    print(f"Placement failed for VNF {vnf} in SFC {sfc}")
                    reward -= 1

        if self.system.calculate_availability() < self.system.min_availability:
            print("Availability constraint violated")
            reward -= 1

        if self.system.calculate_carbon_footprint() > self.system.max_carbon_footprint:
            print("Carbon footprint constraint violated")
            reward -= 1

        reward += self.system.calculate_objective()

        print("Reward:", reward)
        print()

        return reward

    def get_neighbor(self, solution: List[int]) -> List[int]:
        neighbor = solution.copy()
        index = random.randint(0, self.dimension - 1)
        neighbor[index] = random.randint(0, 3)
        return neighbor

    def acceptance_probability(
        self, old_cost: float, new_cost: float, temperature: float
    ) -> float:
        if new_cost > old_cost:
            return 1.0
        return math.exp((new_cost - old_cost) / temperature)

    def optimize(self) -> Tuple[List[int], float]:
        current_solution = self.initialize_solution()
        current_fitness = self.fitness(current_solution)
        best_solution = current_solution
        best_fitness = current_fitness

        temperature = self.initial_temperature

        while temperature > self.min_temperature:
            for _ in range(self.iterations_per_temperature):
                neighbor = self.get_neighbor(current_solution)
                neighbor_fitness = self.fitness(neighbor)

                if (
                    self.acceptance_probability(
                        current_fitness, neighbor_fitness, temperature
                    )
                    > random.random()
                ):
                    current_solution = neighbor
                    current_fitness = neighbor_fitness

                if current_fitness > best_fitness:
                    best_solution = current_solution
                    best_fitness = current_fitness

            temperature *= self.cooling_rate

        return best_solution, best_fitness

    def apply_solution(self, solution: List[int]):
        idx = 0
        self.system.reset()
        self.policy.placement()
        self.system.solution = solution

        print("Solution:", solution)
        candidate_nodes = self.candidate_nodes.copy()

        for sfc in self.system.sfcs:
            primary_vnfs = [vnf for vnf in sfc.vnfs if vnf.backup_of == 0]

            for vnf in primary_vnfs:
                # Safely get number of redundant instances
                if idx >= len(solution):
                    break

                redundant_instances = int(solution[idx])
                idx += 1

                if redundant_instances == 0:
                    continue

                # Try to place redundant VNFs
                self.system.place_redundant_vnf(
                    vnf, redundant_instances, candidate_nodes, sfc
                )

        return self.system
