package com.databricks.labs.remorph

import com.databricks.labs.remorph.intermediate.RemorphError
import com.databricks.labs.remorph.preprocessors.jinja.TemplateManager
import com.fasterxml.jackson.annotation.JsonIgnore

sealed trait WorkflowStage
object WorkflowStage {
  case object PARSE extends WorkflowStage
  case object PLAN extends WorkflowStage
  case object OPTIMIZE extends WorkflowStage
  case object GENERATE extends WorkflowStage
  case object FORMAT extends WorkflowStage
}

/**
 * Represents a stateful computation that will eventually produce an output of type Out. It manages a state of
 * type TranspilerState along the way. Moreover, by relying on the semantics of Result, it is also able to handle
 * errors in a controlled way.
 *
 * It is important to note that nothing won't get evaluated until the run or runAndDiscardState are called.
 * @param run
 *   The computation that will be carried out by this computation. It is basically a function that takes a TranspilerState as
 *   parameter and returns a Result containing the - possibly updated - State along with the Output.
 * @tparam Output
 *   The type of the produced output.
 */
final class Transformation[+Output](val run: TranspilerState => Result[(TranspilerState, Output)]) {

  /**
   * Modify the output of this transformation using the provided function, without changing the managed state.
   *
   * If this transformation results in a KoResult, the provided function won't be evaluated.
   */
  def map[B](f: Output => B): Transformation[B] = new Transformation(run.andThen(_.map { case (s, a) => (s, f(a)) }))

  /**
   * Chain this transformation with another one by passing this transformation's output to the provided function.
   *
   * If this transformation results in a KoResult, the provided function won't be evaluated.
   */
  def flatMap[B](f: Output => Transformation[B]): Transformation[B] = new Transformation(run.andThen {
    case OkResult((s, a)) => f(a).run(s)
    case p @ PartialResult((s, a), err) => p.flatMap { _ => f(a).run(s.recordError(err)) }
    case ko: KoResult => ko
  })

  /**
   * Runs the computation using the provided initial state and return a Result containing the transformation's output,
   * discarding the final state.
   */
  def runAndDiscardState(initialState: TranspilerState): Result[Output] = run(initialState).map(_._2)

  def recoverWith[B](rec: Result[Output] => Transformation[B]): Transformation[B] =
    new Transformation[B](s =>
      run(s) match {
        case ko: KoResult => rec(ko).run(s)
        case PartialResult((s2, out), err) => rec(PartialResult(out, err)).run(s2)
        case OkResult((s2, out)) => rec(OkResult(out)).run(s2)
      })

}

trait TransformationConstructors {

  /**
   * Wraps a value into a successful transformation that ignores its state.
   */
  def ok[A](a: A): Transformation[A] = new Transformation(s => OkResult((s, a)))

  /**
   * Wraps an error into a failed transformation.
   */
  def ko(stage: WorkflowStage, err: RemorphError): Transformation[Nothing] = new Transformation(s =>
    KoResult(stage, err))

  /**
   * Wraps a Result into a transformation that ignores its state.
   */
  def lift[X](res: Result[X]): Transformation[X] = new Transformation(s => res.map(x => (s, x)))

  /**
   * A transformation whose output is the current state.
   */
  @JsonIgnore
  def getCurrentPhase: Transformation[Phase] = new Transformation(s => OkResult((s, s.currentPhase)))

  /**
   * A transformation that replaces the current state with the provided one, and produces no meaningful output.
   */
  def setPhase(newPhase: Phase): Transformation[Unit] = new Transformation(s =>
    OkResult((s.copy(currentPhase = newPhase), ())))

  /**
   * A transformation that updates the current state using the provided partial function, and produces no meaningful
   * output. If the provided partial function cannot be applied to the current state, it remains unchanged.
   */
  def updatePhase(f: PartialFunction[Phase, Phase]): Transformation[Unit] = new Transformation(state => {
    val newState = state.copy(currentPhase = f.applyOrElse(state.currentPhase, identity[Phase]))
    OkResult((newState, ()))
  })

  @JsonIgnore
  def getTemplateManager: Transformation[TemplateManager] = new Transformation(s => OkResult((s, s.templateManager)))

  def updateTemplateManager(updateFunc: TemplateManager => TemplateManager): Transformation[Unit] =
    new Transformation(s => OkResult((s.copy(templateManager = updateFunc(s.templateManager)), ())))
}

sealed trait Result[+A] {
  def map[B](f: A => B): Result[B]
  def flatMap[B](f: A => Result[B]): Result[B]
  def isSuccess: Boolean
  def withNonBlockingError(error: RemorphError): Result[A]
  def getOrElse[B >: A](default: => B): B
}

case class OkResult[A](output: A) extends Result[A] {
  override def map[B](f: A => B): Result[B] = OkResult(f(output))

  override def flatMap[B](f: A => Result[B]): Result[B] = f(output)

  override def isSuccess: Boolean = true

  override def withNonBlockingError(error: RemorphError): Result[A] = PartialResult(output, error)

  override def getOrElse[B >: A](default: => B): B = output
}

case class PartialResult[A](output: A, error: RemorphError) extends Result[A] {

  override def map[B](f: A => B): Result[B] = PartialResult(f(output), error)

  override def flatMap[B](f: A => Result[B]): Result[B] = f(output) match {
    case OkResult(res) => PartialResult(res, error)
    case PartialResult(res, err) => PartialResult(res, RemorphError.merge(error, err))
    case KoResult(stage, err) => KoResult(stage, RemorphError.merge(error, err))
  }

  override def isSuccess: Boolean = true

  override def withNonBlockingError(newError: RemorphError): Result[A] =
    PartialResult(output, RemorphError.merge(error, newError))

  override def getOrElse[B >: A](default: => B): B = output
}

case class KoResult(stage: WorkflowStage, error: RemorphError) extends Result[Nothing] {
  override def map[B](f: Nothing => B): Result[B] = this

  override def flatMap[B](f: Nothing => Result[B]): Result[B] = this

  override def isSuccess: Boolean = false

  override def withNonBlockingError(newError: RemorphError): Result[Nothing] =
    KoResult(stage, RemorphError.merge(error, newError))

  override def getOrElse[B >: Nothing](default: => B): B = default
}
