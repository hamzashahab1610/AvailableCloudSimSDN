from stable_baselines3.common.callbacks import BaseCallback


class StepCallback(BaseCallback):
    def __init__(self, check_freq, verbose=0):
        super(StepCallback, self).__init__(verbose)
        self.check_freq = check_freq

    def _on_step(self) -> bool:
        if self.n_calls % self.check_freq == 0:
            print(
                f"Step number: {self.n_calls}, Action: {self.locals['actions']},current reward: {self.locals['rewards']}"
            )

        return True
