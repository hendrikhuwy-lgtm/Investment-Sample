import { Router, type IRouter } from "express";
import healthRouter from "./health";
import briefRouter from "./brief";
import blueprintRouter from "./blueprint";

const router: IRouter = Router();

router.use(healthRouter);
router.use(briefRouter);
router.use(blueprintRouter);

export default router;
