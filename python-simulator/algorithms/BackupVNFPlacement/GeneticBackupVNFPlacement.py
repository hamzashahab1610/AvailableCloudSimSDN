import random
from typing import List, Tuple


class GeneticBackupVNFPlacement:
    def __init__(
        self,
        policy,
        population_size=100,
        generations=50,
        mutation_rate=0.1,
        selection_rate=0.2,
    ):
        self.policy = policy
        self.system = self.policy.placement()
        self.candidate_nodes = sorted(
            self.system.nodes,
            key=lambda node: self.policy.get_node_score(node),
        )

        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.selection_rate = selection_rate
        self.chromosome_length = sum(len(sfc.vnfs) for sfc in self.system.sfcs)

    def initialize_population(self) -> List[List[int]]:
        return [
            [random.randint(1, 2) for _ in range(self.chromosome_length)]
            for _ in range(self.population_size)
        ]

    def fitness(self, chromosome: List[int]) -> float:
        self.system.reset()
        self.policy.placement()

        reward = 0
        idx = 0

        print("Solution:", chromosome)
        candidate_nodes = self.candidate_nodes.copy()

        for sfc in self.system.sfcs:
            primary_vnfs = [vnf for vnf in sfc.vnfs if vnf.backup_of == 0]

            for vnf in primary_vnfs:
                # Safely get number of redundant instances
                if idx >= len(chromosome):
                    break

                redundant_instances = int(chromosome[idx])
                idx += 1

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

    def selection(
        self, population: List[List[int]], fitnesses: List[float]
    ) -> List[List[int]]:
        population_with_fitness = list(zip(population, fitnesses))

        population_with_fitness.sort(key=lambda x: x[1], reverse=True)

        num_parents = int(self.population_size * self.selection_rate)

        parents = [chrom for chrom, _ in population_with_fitness[:num_parents]]

        return parents

    def crossover(
        self, parent1: List[int], parent2: List[int]
    ) -> Tuple[List[int], List[int]]:
        crossover_point = random.randint(1, self.chromosome_length - 1)

        child1 = parent1[:crossover_point] + parent2[crossover_point:]
        child2 = parent2[:crossover_point] + parent1[crossover_point:]

        return child1, child2

    def mutation(self, chromosome: List[int]) -> List[int]:
        return [
            random.randint(0, 3) if random.random() < self.mutation_rate else gene
            for gene in chromosome
        ]

    def evolve(self) -> List[int]:
        population = self.initialize_population()

        for _ in range(self.generations):
            fitnesses = [self.fitness(chrom) for chrom in population]
            parents = self.selection(population, fitnesses)

            next_generation = []

            for i in range(0, int(self.population_size * self.selection_rate), 2):
                parent1, parent2 = parents[i], parents[i + 1]
                child1, child2 = self.crossover(parent1, parent2)
                next_generation.extend([self.mutation(child1), self.mutation(child2)])

            population = next_generation

        best_chromosome = max(population, key=self.fitness)
        return best_chromosome

    def optimize(self) -> Tuple[List[int], float]:
        best_chromosome = self.evolve()
        best_fitness = self.fitness(best_chromosome)
        return best_chromosome, best_fitness

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
